LIBDIR=`erl -eval 'io:format("~s~n", [code:lib_dir()])' -s init stop -noshell`
VERSION=0.2

all:
	mkdir -p ebin
	(cd src;$(MAKE))

edoc:
	(cd src;$(MAKE) edoc)

test: all
	prove t/*.t

clean:
	(cd src;$(MAKE) clean)

dist-src:
	mkdir mochiweb-$(VERSION)/ && cp -rfv Makefile README priv scripts src support mochiweb-$(VERSION)/
	tar zcf mochiweb-$(VERSION).tgz mochiweb-$(VERSION)

install: all
	mkdir -p ${LIBDIR}/mochiweb-$(VERSION)/{ebin,include}
	for i in ebin/*.beam; do install $$i $(LIBDIR)/mochiweb-$(VERSION)/$$i ; done
