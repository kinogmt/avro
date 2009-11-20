%%%-------------------------------------------------------------------
%%% File    : avro_serde.erl
%%% Author  : Todd Lipcon <todd@lipcon.org>
%%% Description : 
%%%
%%% Created : 19 Nov 2009 by Todd Lipcon <todd@lipcon.org>
%%%-------------------------------------------------------------------
-module(avro_serde).

-compile(export_all).

-include("avro_schema.hrl").

%%%%%%%%% ENCODING

% Primitive Types
encode(int, Int) ->
    Zig = zigzag_encode(int, Int),
    varint_encode(<<Zig:32>>);
encode(long, Long) ->
    Zig = zigzag_encode(long, Long),
    varint_encode(<<Zig:64>>);
encode(string, Data) when is_binary(Data) -> % TODO new erl unicode stuff?
    [encode(long, iolist_size(Data)), Data];
encode(bytes, Data) when is_binary(Data) ->
    [encode(long, iolist_size(Data)), Data];
encode(float, Float) when is_float(Float) ->
    <<Float:32/little-float>>;
encode(double, Double) when is_float(Double) ->
    <<Double:64/little-float>>;
encode(boolean, true) -> <<1>>;
encode(boolean, false) -> <<0>>;
encode(null, undefined) -> <<>>;
encode(null, null) -> <<>>;

% Records
encode(#avro_record{fields=Fields},
       Data) when is_tuple(Data) orelse is_list(Data) ->
    NFields = length(Fields),
    if is_tuple(Data), size(Data) == NFields ->
            [encode(Type, FieldData) ||
                {Type, FieldData} <- lists:zip(Fields, tuple_to_list(Data))];
       is_tuple(Data), size(Data) == NFields + 1, is_atom(element(1, Data)) ->
            % Probably a record
            [_RecordName | RealData] = tuple_to_list(Data),
            [encode(Type, FieldData) ||
                {{_FieldName, Type}, FieldData} <- lists:zip(Fields, RealData)]
    end;

% Enums
encode(#avro_enum{symbols=Symbols},
       Symbol) when is_atom(Symbol) ->
    case search_elem(Symbol, Symbols) of
        N when is_integer(N) -> encode(int, N)
    end;

% Unions
encode(#avro_union{types=Types},
       {Type, Val}) ->
    case search_elem(Type, Types) of
        N when is_integer(N) ->
            [encode(long, N),
             encode(Type, Val)]
    end.

%%%%%%%% DECODING

decode(int, Bin) ->
    {Zig, Rest} = varint_decode(Bin), % TODO not diff between int and long
    {zigzag_decode(int, Zig), Rest};
decode(long, Bin) ->
    {Zig, Rest} = varint_decode(Bin),  % TODO not diff between int and long
    {zigzag_decode(long, Zig), Rest};
decode(string, Bin) ->  % TODO new erl unicode stuff?
    {Length, Rest} = decode(long, Bin),
    <<Data:Length/binary, Rest2/binary>> = Rest,
    {Data, Rest2};
decode(bytes, Bin) ->
    {Length, Rest} = decode(long, Bin),
    <<Data:Length/binary, Rest2/binary>> = Rest,
    {Data, Rest2};
decode(float, <<Float:32/little-float, Rest/binary>>) ->
    {Float, Rest};
decode(double, <<Double:64/little-float, Rest/binary>>) ->
    {Double, Rest};
decode(boolean, <<1, Rest/binary>>) -> {true, Rest};
decode(boolean, <<0, Rest/binary>>) -> {false, Rest};
decode(null, Bin) when is_binary(Bin) -> {null, Bin}; % TODO null or undefined?

% Records
decode(#avro_record{fields=Fields}, Bin) ->
    % TODO this decodes records as tuples... is this what we want?
    % or do we want proplists? or dicts?
    {RevDataList, Rest} =
        lists:foldl(fun({_FieldName, Type}, {AccData, BinIn}) ->
                            {Data, Rest} = decode(Type, BinIn),
                            {[Data | AccData], Rest}
                    end,
                    {[], Bin},
                    Fields),
    {list_to_tuple(lists:reverse(RevDataList)), Rest};

% Enums
decode(#avro_enum{symbols=Symbols}, Bin) ->
    {SymIndex, Rest} = decode(int, Bin),
    {lists:nth(SymIndex + 1, Symbols), Rest};

% Union
decode(#avro_union{types=Types}, Bin) ->
    {TypeIndex, Rest} = decode(long, Bin),
    Type = lists:nth(TypeIndex + 1, Types),
    {DecodedVal, Rest2} = decode(Type, Rest),
    {{Type, DecodedVal}, Rest2}.

%%%%% INTEGER ENCODING/DECODING %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% 64 bit encode
varint_encode(<<0:64>>) -> <<0>>;
varint_encode(<<0:57, B1:7>>) -> <<B1>>;
varint_encode(<<0:50, B1:7, B2:7>>) ->
    <<1:1, B2:7, B1>>;
varint_encode(<<0:43, B1:7, B2:7, B3:7>>) ->
    <<1:1, B3:7, 1:1, B2:7, B1>>;
varint_encode(<<0:36, B1:7, B2:7, B3:7, B4:7>>) ->
    <<1:1, B4:7, 1:1, B3:7, 1:1, B2:7, B1>>;
varint_encode(<<0:29, B1:7, B2:7, B3:7, B4:7, B5:7>>) ->
    <<1:1, B5:7, 1:1, B4:7, 1:1, B3:7, 1:1, B2:7, B1>>;
varint_encode(<<0:22, B1:7, B2:7, B3:7, B4:7, B5:7, B6:7>>) ->
    <<1:1, B6:7, 1:1, B5:7, 1:1, B4:7, 1:1, B3:7, 1:1, B2:7, B1>>;
varint_encode(<<0:15, B1:7, B2:7, B3:7, B4:7, B5:7, B6:7, B7:7>>) ->
    <<1:1, B7:7, 1:1, B6:7, 1:1, B5:7, 1:1, B4:7, 1:1, B3:7, 1:1, B2:7, B1>>;
varint_encode(<<0:8, B1:7, B2:7, B3:7, B4:7, B5:7, B6:7, B7:7, B8:7>>) ->
    <<1:1, B8:7, 1:1, B7:7, 1:1, B6:7, 1:1, B5:7, 1:1, B4:7, 1:1, B3:7, 1:1, B2:7, B1>>;
varint_encode(<<0:1, B1:7, B2:7, B3:7, B4:7, B5:7, B6:7, B7:7, B8:7, B9:7>>) ->
    <<1:1, B9:7, 1:1, B8:7, 1:1, B7:7, 1:1, B6:7, 1:1, B5:7, 1:1, B4:7, 1:1, B3:7, 1:1, B2:7, B1>>;
varint_encode(<<B1:1, B2:7, B3:7, B4:7, B5:7, B6:7, B7:7, B8:7, B9:7, B10:7>>) ->
    <<1:1, B10:7, 1:1, B9:7, 1:1, B8:7, 1:1, B7:7, 1:1, B6:7, 1:1, B5:7, 1:1, B4:7, 1:1, B3:7, 1:1, B2:7, B1>>;


% 32 bit encode
varint_encode(<<0:32>>) -> <<0>>;
varint_encode(<<0:25, B1:7>>) -> <<B1>>;
varint_encode(<<0:18, B1:7, B2:7>>) ->
    <<1:1, B2:7, B1>>;
varint_encode(<<0:11, B1:7, B2:7, B3:7>>) ->
    <<1:1, B3:7, 1:1, B2:7, B1>>;
varint_encode(<<0:4, B1:7, B2:7, B3:7, B4:7>>) ->
    <<1:1, B4:7, 1:1, B3:7, 1:1, B2:7, B1>>;
varint_encode(<<B1:4, B2:7, B3:7, B4:7, B5:7>>) ->
    <<1:1, B5:7, 1:1, B4:7, 1:1, B3:7, 1:1, B2:7, B1>>.

% decode
varint_decode(Bin) ->
    varint_decode(Bin, []).
varint_decode(<<1:1, Bits:7, Rest/binary>>, Acc) ->
    varint_decode(Rest, [Bits | Acc]);
varint_decode(<<0:1, NewBits:7, Rest/binary>>, Acc) ->
    NewAcc = [NewBits | Acc],
    %% Now we have the bits in most sig to least sig
    Decoded = lists:foldl(fun(Bits, AccIn) ->
                        AccIn bsl 7 + Bits
                end,
                0,
                NewAcc),
    {Decoded, Rest}.

% Zigzag encode/decode
zigzag_encode(int, Int) ->
    (Int bsl 1) bxor (Int bsr 31);
zigzag_encode(long, Int) ->
    (Int bsl 1) bxor (Int bsr 63).

zigzag_decode(int, ZigInt) ->
    (ZigInt bsr 1) bxor -(ZigInt band 1);
zigzag_decode(long, ZigInt) ->
    (ZigInt bsr 1) bxor -(ZigInt band 1).


% Utility function - return the index of an element in a list
search_elem(Elem, List) ->
    search_elem(Elem, List, 0).
search_elem(_Elem, [], _Index) -> not_found;
search_elem(Elem, [Elem | _T], Index) ->
    Index;
search_elem(Elem, [_H | T], Index) ->
    search_elem(Elem, T, Index + 1).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% TESTING %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

test() ->
    ok = test_int_encoding(),
    ok = test_int_decoding(),
    ok = test_string_encoding(),
    ok = test_string_decoding(),
    ok = test_float_double_serde(),
    ok = test_bool_serde(),
    ok = test_record_encoding(),
    ok = test_record_decoding(),
    ok = test_enum_encoding(),
    ok = test_enum_decoding(),
    ok = test_union_encoding(),
    ok = test_union_decoding().

% Test int encoding from examples in avro spec
test_int_encoding() ->
    <<0>> = encode(int, 0),
    <<3>> = encode(int, -2),
    <<16#7f>> = encode(int, -64),
    <<128,1>> = encode(int, 64),
    ok.

test_int_decoding() ->
    {0, <<>>} = decode(int, <<0>>),
    {-2, <<>>} = decode(int, <<3>>),
    {-64, <<>>} = decode(int, <<16#7f>>),
    {64, <<>>} = decode(int, <<128, 1>>),
    ok.


test_string_encoding() ->
    <<10, "hello">> = iolist_to_binary(encode(string, <<"hello">>)),
    <<10, "hello">> = iolist_to_binary(encode(bytes, <<"hello">>)),
    ok.

test_string_decoding() ->
    {<<"hello">>, <<>>} = decode(string, <<10, "hello">>),
    {<<"hello">>, <<>>} = decode(bytes, <<10, "hello">>),
    ok.


test_float_double_serde() ->
    EncFloat = encode(float, 234.0),
    {234.0, <<>>} = decode(float, EncFloat),

    EncDouble = encode(double, 234.0),
    {234.0, <<>>} = decode(double, EncDouble),
    ok.

test_bool_serde() ->
    <<1>> = encode(boolean, true),
    <<0>> = encode(boolean, false),
    {true, <<>>} = decode(boolean, <<1>>),
    {false, <<>>} = decode(boolean, <<0>>),
    ok.

test_record_encoding() ->
    % Example from avro spec
    Schema = #avro_record{
      name= <<"test">>,
      fields=[{<<"a">>, long},
              {<<"b">>, string}]},
    % An instance of this record whose a field has value 27 (encoded
    % as hex 36) and whose b field has value "foo" (encoded as hex bytes
    % O6 66 6f 6f), would be encoded simply as the concatenation of these,
    % namely the hex byte sequence: 36 06 66 6f 6f
    <<16#36, 16#06, 16#66, 16#6f, 16#6f>> =
        iolist_to_binary(encode(Schema, {test, 27, <<"foo">>})),
    ok.


test_record_decoding() ->
    % Same example from avro spec as above
    Schema = #avro_record{
      name= <<"test">>,
      fields=[{<<"a">>, long},
              {<<"b">>, string}]},
    {{27, <<"foo">>}, <<>>} = decode(Schema, <<16#36, 16#06, 16#66, 16#6f, 16#6f>>),
    ok.


test_enum_encoding() ->
    <<2>> = encode(#avro_enum{symbols=[foo, bar, baz]}, bar),
    <<0>> = encode(#avro_enum{symbols=[foo, bar, baz]}, foo),
    ok.

test_enum_decoding() ->
    {bar, <<>>} = decode(#avro_enum{symbols=[foo, bar, baz]}, <<2>>),
    {foo, <<>>} = decode(#avro_enum{symbols=[foo, bar, baz]}, <<0>>),
    % TODO what should we do with unknown enum?
    ok.

test_union_encoding() ->
    Schema = #avro_union{types = [string, null]}, % TODO should this be tuple?
    <<0, 6, "foo">> = iolist_to_binary(encode(Schema, {string, <<"foo">>})),
    <<2>> = iolist_to_binary(encode(Schema, {null, null})), % TODO this kinda weird
    ok.

test_union_decoding() ->
    Schema = #avro_union{types = [string, null]}, % TODO should this be tuple?
    {{string, <<"foo">>}, <<>>} = decode(Schema, <<0, 6, "foo">>),
    ok.
