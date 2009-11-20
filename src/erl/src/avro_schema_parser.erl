%%% File    : avro_schema_parser.erl
%%% Author  : Todd Lipcon <todd@lipcon.org>
%%% Description : 
%%% Created : 19 Nov 2009 by Todd Lipcon <todd@lipcon.org>

-module(avro_schema_parser).
-include("avro_schema.hrl").

-export([parse_schema_file/1, parse_schema/1, test/0]).


parse_schema_file(SchemaPath) ->
    {{ok, Schema}, SchemaPath} = {file:read_file(SchemaPath), SchemaPath},
    parse_schema(Schema).

parse_schema(SchemaBin) when is_binary(SchemaBin) ->
    ParsedJson = mochijson2:decode(SchemaBin),
    % io:format("JSON: ~p~n", [ParsedJson]),
    walk_json_tree(ParsedJson).



walk_json_tree(<<"string">>) -> string;
walk_json_tree(<<"bytes">>) -> bytes;
walk_json_tree(<<"int">>) -> int;
walk_json_tree(<<"long">>) -> long;
walk_json_tree(<<"float">>) -> float;
walk_json_tree(<<"double">>) -> double;
walk_json_tree(<<"boolean">>) -> boolean;
walk_json_tree(<<"null">>) -> null;
% union
walk_json_tree(Types) when is_list(Types) ->
    {union, [walk_json_tree(Type) || Type <- Types]};
% record/array
walk_json_tree({struct, JsonFields}) when is_list(JsonFields) ->
    Type = proplists:get_value(<<"type">>, JsonFields),
    parse_special(Type, JsonFields);
walk_json_tree(TypeReference) when is_binary(TypeReference) ->
    {type_reference, TypeReference}.

parse_special(<<"record">>, JsonFields) -> parse_record(JsonFields);
parse_special(<<"array">>, JsonFields) -> parse_array(JsonFields);
parse_special(<<"enum">>, JsonFields) -> parse_enum(JsonFields);
parse_special(<<"map">>, JsonFields) -> parse_map(JsonFields);
parse_special(<<"fixed">>, JsonFields) -> parse_fixed(JsonFields).

parse_record(JsonFields) ->
    [Name] = proplists:get_all_values(<<"name">>, JsonFields),
    [RecFields] = proplists:get_all_values(<<"fields">>, JsonFields),
    Namespace = proplists:get_value(<<"namespace">>, JsonFields),
    #avro_record{name=Name,
                 namespace=Namespace,
                 fields=[parse_record_field(Field) || Field <- RecFields]}.

parse_record_field({struct, JFields}) ->
    [FieldName] = proplists:get_all_values(<<"name">>, JFields),
    [Type] = proplists:get_all_values(<<"type">>, JFields),
    % TODO(todd) default
    {FieldName, walk_json_tree(Type)}.

parse_enum(JsonFields) ->
    [Name] = proplists:get_all_values(<<"name">>, JsonFields),
    [Symbols] = proplists:get_all_values(<<"symbols">>, JsonFields),
    {enum, Name, [list_to_atom(binary_to_list(Sym)) || Sym <- Symbols]}.

parse_array(JsonFields) ->
    [ItemType] = proplists:get_all_values(<<"items">>, JsonFields),
    {array, walk_json_tree(ItemType)}.

parse_map(JsonFields) ->
    [ItemType] = proplists:get_all_values(<<"values">>, JsonFields),
    {map, walk_json_tree(ItemType)}.

parse_fixed(JsonFields) ->
    [Name] = proplists:get_all_values(<<"name">>, JsonFields),
    [Size] = proplists:get_all_values(<<"size">>, JsonFields),
    if is_integer(Size), Size > 0 -> {fixed, Name, Size};
       true -> error_logger:error_msg("Invalid size: ~p~n", Size)
    end.

test() ->
    AllTests = all_tests(),
    run_tests(AllTests).

run_tests([]) ->
    ok;
run_tests([Test | Rest]) ->
    run_test(test_path(Test),
             test_path(re:replace(Test, "avsc\$", "erl", [{return,list}]))),
    run_tests(Rest).

run_test(SchemaPath, ErlangPath) ->
    {{ok, [ErlangData]}, ErlangPath} = {file:consult(ErlangPath), ErlangPath},
    {{ok, Schema}, SchemaPath} = {file:read_file(SchemaPath), SchemaPath},
    Parsed = parse_schema(Schema),
    case Parsed of
        ErlangData ->
            io:format("Test ~p passed!~n", [SchemaPath]),
            ok;
        _ ->
            io:format("Test ~p failed!~n", [SchemaPath]),
            io:format("Parsed as: ~s~nExpected: ~s~n",
                      [io_lib_pretty:print(Parsed),
                       io_lib_pretty:print(ErlangData)]),
            fail
    end.


all_tests() ->
    {ok, Names} = file:list_dir(test_base()),
    [N || N <- Names,
          re:run(N, "^[^.].*\.avsc\$") =/= nomatch].

test_path(Path) ->
    filename:join([test_base(), Path]).

test_base() ->
    SourcePath = proplists:get_value(source, module_info(compile)),
    filename:join([filename:dirname(SourcePath), "..", "testdata"]).
