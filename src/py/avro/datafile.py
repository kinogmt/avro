# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Read/Write Avro Data Files"""

import struct, uuid, cStringIO
import avro.schema as schema
import avro.io as io

#Data file constants.
_VERSION = 0
_MAGIC = "Obj"+chr(_VERSION)
_SYNC_SIZE = 16
_SYNC_INTERVAL = 1000*_SYNC_SIZE
_FOOTER_BLOCK = -1
class DataFileWriter(object):
  """Stores in a file a sequence of data conforming to a schema. The schema is 
  stored in the file with the data. Each datum in a file is of the same
  schema. Data is grouped into blocks.
  A synchronization marker is written between blocks, so that
  files may be split. Blocks may be compressed. Extensible metadata is
  stored at the end of the file. Files may be appended to."""

  def __init__(self, schm, writer, dwriter):
    self.__writer = writer
    self.__encoder = io.Encoder(writer)
    self.__dwriter = dwriter
    self.__dwriter.setschema(schm)
    self.__count = 0  #entries in file
    self.__blockcount = 0  #entries in current block
    self.__buffer = cStringIO.StringIO()
    self.__bufwriter = io.Encoder(self.__buffer)
    self.__meta = dict()
    self.__sync = uuid.uuid4().bytes
    self.__meta["sync"] = self.__sync
    self.__meta["codec"] = "null"
    self.__meta["schema"] = schema.stringval(schm)
    self.__writer.write(struct.pack(len(_MAGIC).__str__()+'s',
                                    _MAGIC))

  def setmeta(self, key, val):
    """Set a meta data property."""
    self.__meta[key] = val

  def append(self, datum):
    """Append a datum to the file."""
    self.__dwriter.write(datum, self.__bufwriter)
    self.__count+=1
    self.__blockcount+=1
    if self.__buffer.tell() >= _SYNC_INTERVAL:
      self.__writeblock()

  def __writeblock(self):
    if self.__blockcount > 0:
      self.__writer.write(self.__sync)
      self.__encoder.writelong(self.__blockcount)
      self.__writer.write(self.__buffer.getvalue())
      self.__buffer.truncate(0) #reset
      self.__blockcount = 0

  def sync(self):
    """Return the current position as a value that may be passed to
    DataFileReader.seek(long). Forces the end of the current block,
    emitting a synchronization marker."""
    self.__writeblock()
    return self.__writer.tell()

  def flush(self):
    """Flush the current state of the file, including metadata."""
    self.__writefooter()
    self.__writer.flush()

  def close(self):
    """Close the file."""
    self.flush()
    self.__writer.close()

  def __writefooter(self):
    self.__writeblock()
    self.__meta["count"] = self.__count.__str__()
    
    self.__bufwriter.writelong(len(self.__meta))
    for k,v in self.__meta.items():
      self.__bufwriter.writeutf8(unicode(k,'utf-8'))
      self.__bufwriter.writebytes(str(v))
    size = self.__buffer.tell() + 4
    self.__writer.write(self.__sync)
    self.__encoder.writelong(_FOOTER_BLOCK)
    self.__encoder.writelong(size)
    self.__buffer.flush()
    self.__writer.write(self.__buffer.getvalue())
    self.__buffer.truncate(0) #reset
    self.__writer.write(chr((size >> 24) & 0xFF))
    self.__writer.write(chr((size >> 16) & 0xFF))
    self.__writer.write(chr((size >> 8) & 0xFF))
    self.__writer.write(chr((size >> 0) & 0xFF))

class DataFileReader(object):
  """Read files written by DataFileWriter."""

  def __init__(self, reader, dreader):
    self.__reader = reader
    self.__decoder = io.Decoder(reader)
    mag = struct.unpack(len(_MAGIC).__str__()+'s', 
                 self.__reader.read(len(_MAGIC)))[0]
    if mag != _MAGIC:
      raise schema.AvroException("Not an avro data file")
    #find the length
    self.__reader.seek(0,2)
    self.__length = self.__reader.tell()
    self.__reader.seek(-4, 2)
    footersize = (int(ord(self.__reader.read(1)) << 24) +
            int(ord(self.__reader.read(1)) << 16) +
            int(ord(self.__reader.read(1)) << 8) +
            int(ord(self.__reader.read(1))))
    seekpos = self.__reader.seek(self.__length-footersize)
    metalength = self.__decoder.readlong()
    if metalength < 0:
      metalength = -metalength
      self.__decoder.readlong() #ignore byteCount if this is a blocking map
    self.__meta = dict()
    for i in range(0, metalength):
      key = self.__decoder.readutf8()
      self.__meta[key] = self.__decoder.readbytes()
    self.__sync = self.__meta.get("sync")
    self.__count = int(self.__meta.get("count"))
    self.__codec = self.__meta.get("codec")
    if (self.__codec != None) and (self.__codec != "null"):
      raise schema.AvroException("Unknown codec: " + self.__codec)
    self.__schema = schema.parse(self.__meta.get("schema").encode("utf-8"))
    self.__blockcount = 0
    self.__dreader = dreader
    self.__dreader.setschema(self.__schema)
    self.__reader.seek(len(_MAGIC))

  def __iter__(self):
    return self

  def getmeta(self, key):
    """Return the value of a metadata property."""
    return self.__meta.get(key)

  def next(self):
    """Return the next datum in the file."""
    while self.__blockcount == 0:
      if self.__reader.tell() == self.__length:
        raise StopIteration
      self.__skipsync()
      self.__blockcount = self.__decoder.readlong()
      if self.__blockcount == _FOOTER_BLOCK:
        self.__reader.seek(self.__decoder.readlong()+self.__reader.tell())
        self.__blockcount = 0
    self.__blockcount-=1
    datum = self.__dreader.read(self.__decoder)
    return datum

  def __skipsync(self):
    if self.__reader.read(_SYNC_SIZE)!=self.__sync:
      raise schema.AvroException("Invalid sync!")

  def seek(self, pos):
    """Move to the specified synchronization point, as returned by 
    DataFileWriter.sync()."""
    self.__reader.seek(pos)
    self.__blockcount = 0

  def sync(self, position):
    """Move to the next synchronization point after a position."""
    if self.__reader.tell()+_SYNC_SIZE >= self.__length:
      self.__reader.seek(self.__length)
      return
    self.__reader.seek(position)
    self.__reader.read(_SYNC_SIZE)

  def close(self):
    """Close this reader."""
    self.__reader.close()
