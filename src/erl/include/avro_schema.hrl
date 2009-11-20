-ifndef(AVRO_SCHEMA_HRL).
-define(AVRO_SCHEMA_HRL, yea).

-record(avro_record,
        {name, namespace, fields}).

-record(avro_enum,
        {name, symbols}).

-endif.
