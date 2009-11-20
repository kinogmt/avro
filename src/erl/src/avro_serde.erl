%%%-------------------------------------------------------------------
%%% File    : avro_serde.erl
%%% Author  : Todd Lipcon <todd@lipcon.org>
%%% Description : 
%%%
%%% Created : 19 Nov 2009 by Todd Lipcon <todd@lipcon.org>
%%%-------------------------------------------------------------------
-module(avro_serde).

-compile(export_all).

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
encode(boolean, false) -> <<0>>.

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
decode(boolean, <<0, Rest/binary>>) -> {false, Rest}.

%%%%% INTEGER ENCODING/DECODING %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
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


zigzag_encode(int, Int) ->
    (Int bsl 1) bxor (Int bsr 31);
zigzag_encode(long, Int) ->
    (Int bsl 1) bxor (Int bsr 63).

zigzag_decode(int, ZigInt) ->
    (ZigInt bsr 1) bxor -(ZigInt band 1);
zigzag_decode(long, ZigInt) ->
    (ZigInt bsr 1) bxor -(ZigInt band 1).



test() ->
    ok = test_int_encoding(),
    ok = test_int_decoding(),
    ok = test_string_encoding(),
    ok = test_string_decoding(),
    ok = test_float_double_serde(),
    ok = test_bool_serde().

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
