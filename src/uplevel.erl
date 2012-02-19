-module(uplevel).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(TESTDB, "/tmp/uplevel.test").
-endif.

-export([
	handle/1, handle/2,
    put/5,
    put_command/3, put_command/4,
	delete_command/2, delete_command/3,
	get/4,
    delete/3,
    delete/4,
    write/2, write/3,
    range/4, range/5,
    next/3,
    next_larger/3
]).

-type table_handle() :: any().
-type put_options()  :: [{put_options, []} | {key_encoder, fun()}] | [].
-type get_options()  :: [{key_decoder, fun()}] | [].
-type delete_options()  :: [{sync, boolean()}] | [].
-type keys_options()  :: [{key_encoder, fun()} | {key_decoder, fun()}] | [].
-type max_key() :: binary() | function().
-type command() :: {put, binary(), binary()} | {delete, binary()}.

-define(SEPARATOR, <<"/uplevel-separator/">>).
-define(MINBINARY, <<0:8>>).

% creates a table and returns the table handle
-spec handle(string()) -> table_handle().
handle(Path) ->
	handle(Path, [{create_if_missing, true}]).

-spec handle(string(), list()) -> table_handle().
handle(Path, Options) ->
	{ok, Handle} = eleveldb:open(Path, Options),
	Handle.

-spec put(binary(), any(), any(), table_handle(), put_options()) -> ok | {error, any()}.
put(Bucket, Key, Value, Handle, Options) ->
    eleveldb:write(Handle, [put_command(Bucket, Key, Value, Options)], Options).

-spec put_command(binary(), any(), any()) -> ok | {error, any()}.
put_command(Bucket, Key, Value) ->
    put_command(Bucket, Key, Value, []).

-spec put_command(binary(), any(), any(), put_options()) -> ok | {error, any()}.
put_command(Bucket, Key, Value, Options) ->
	KeyEncoded =  encode_key(Key, 		 proplists:get_value(key_encoder, Options)),
	KeyPrefixed = prefix_key(KeyEncoded, Bucket),
    {put, KeyPrefixed, term_to_binary(Value)}.

-spec get(binary(), any(), table_handle(), get_options()) -> 'not_found' | {binary(), binary()}.
get(Bucket, Key, Handle, Options) ->
	KeyEncoded =  encode_key(Key, 		 	proplists:get_value(key_encoder, Options)),
	KeyPrefixed = prefix_key(KeyEncoded, 	Bucket),
	case eleveldb:get(Handle, KeyPrefixed,  proplists:get_value(get_options, Options, [])) of
		{ok, Value} -> {Key, binary_to_term(Value)};
		not_found -> not_found
	end.

-spec delete(binary(), any(), table_handle()) -> ok.
delete(Bucket, Key, Handle) ->
    delete(Bucket, Key, Handle, []).

-spec delete(binary(), any(), table_handle(), delete_options()) -> ok.
delete(Bucket, Key, Handle, Options) ->
    eleveldb:write(Handle, [delete_command(Bucket, Key, Options)], Options).

-spec delete_command(binary(), any()) -> ok | {error, any()}.
delete_command(Bucket, Key) -> 
    delete_command(Bucket, Key, []).

-spec delete_command(binary(), any(), put_options()) -> ok | {error, any()}.
delete_command(Bucket, Key, Options) -> 
    KeyEncoded =  encode_key(Key,           proplists:get_value(key_encoder, Options)),
    KeyPrefixed = prefix_key(KeyEncoded,    Bucket),
    {delete, KeyPrefixed}.

-spec write([command()], any()) -> ok | {error, any()}.
write(Commands, Handle) ->
    write(Handle, Commands, []).

-spec write([command()], any(), delete_options()) -> ok | {error, any()}.
write(Commands, Handle, Options) ->
    eleveldb:write(Handle, Commands, Options).

-spec range(binary(), binary(), max_key(), table_handle()) -> [binary()].
range(Bucket, Key, Max, Handle) ->
    range(Bucket, Key, Max, Handle, []).

-spec range(binary(), binary(), max_key(), table_handle(), keys_options()) -> [binary()].
range(Bucket, KeyMin, Max, Handle, Options) ->
    % check if the encoding options make sense
    {Encoder, Decoder} = {proplists:get_value(key_encoder, Options), proplists:get_value(key_decoder, Options)},
    if
        (Encoder =:= undefined) xor (Decoder =:= undefined) ->
            throw({both_encoder_and_decoder_needed, [{key_encoder, Encoder}, {key_decoder, Decoder}]});
        true ->
            nothing
    end,
    {ok, Iterator} = eleveldb:iterator(Handle, []),
    Keys = 
    case {Encoder, Decoder} of
        {undefined, undefined} ->
            KeyMinComposite = prefix_key(KeyMin, Bucket),
            if
                is_function(Max) ->
                    case Max(KeyMin) of
                        false -> [];
                        true ->  next_key_fun(Iterator, Max, eleveldb:iterator_move(Iterator, KeyMinComposite), [])
                    end;
                is_binary(Max) andalso KeyMin > Max -> [];
                is_binary(Max) -> next_key_max(Iterator, Max, eleveldb:iterator_move(Iterator, KeyMinComposite), []);
                true -> throw({malformed_max_value, Max})
            end;
        {Encoder, Decoder} ->
            KeyMinComposite = prefix_key(encode_key(KeyMin, Encoder), Bucket),
            KeysEncodedValues =
            if
                is_function(Max) ->
                    case Max(KeyMin) of
                        false -> [];
                        true ->  next_key_fun(Iterator, Max, eleveldb:iterator_move(Iterator, KeyMinComposite), [], Decoder)
                    end;
                true ->
                    case encode_key(KeyMin, Encoder) > encode_key(Max, Encoder) of
                        true  -> [];
                        false -> next_key_max(Iterator, encode_key(Max, Encoder), eleveldb:iterator_move(Iterator, KeyMinComposite), [])
                    end
            end,
            [{Decoder(K), V} || {K, V} <- KeysEncodedValues]
    end,
    eleveldb:iterator_close(Iterator),
    lists:reverse(Keys).

% get keys until the key equals a given key
next_key_max(Iterator, Max, Candidate, KeysValues) ->
    case Candidate of
        {ok, CompositeKeyNext, ValueNext} ->
            {_Bucket, Key} = expand_key(CompositeKeyNext),
            case Key of
                _ when Key =< Max ->
                    next_key_max(Iterator, Max, eleveldb:iterator_move(Iterator, next), [{Key, binary_to_term(ValueNext)} | KeysValues]);
                _ -> KeysValues
            end;
        {error, invalid_iterator}   -> KeysValues
    end.

next_larger(Bucket, KeyMin, Handle) ->
    next(Bucket, <<KeyMin/binary, ?MINBINARY/binary>>, Handle).

next(Bucket, KeyMin, Handle) ->
    {ok, Iterator} = eleveldb:iterator(Handle, []),
    case eleveldb:iterator_move(Iterator, prefix_key(KeyMin, Bucket)) of
        {ok, CompositeKey, Value} ->
            {BucketIn, Key} = expand_key(CompositeKey),
            case BucketIn of
                Bucket ->
                    {Key, binary_to_term(Value)};
                _ ->
                    not_found
            end;
        {error,invalid_iterator} ->
            not_found
    end.

% get keys until the key equals a given key
next_key_fun(Iterator, MaxFun, Candidate, KeysValues) ->
    case Candidate of
        {ok, CompositeKeyNext, ValueNext} ->
            {_Bucket, Key} = expand_key(CompositeKeyNext),
            case MaxFun(Key) of
                true -> next_key_fun(Iterator, MaxFun, eleveldb:iterator_move(Iterator, next), [{Key, binary_to_term(ValueNext)} | KeysValues]);
                false -> KeysValues
            end;
        {error, invalid_iterator}   -> KeysValues
    end.

% get keys until the key equals a given key
next_key_fun(Iterator, MaxFun, Candidate, KeysValues, Decoder) ->
    case Candidate of
        {ok, CompositeKeyNext, ValueNext} ->
            {_Bucket, Key} = expand_key(CompositeKeyNext),
            case MaxFun(Decoder(Key)) of
                true -> next_key_fun(Iterator, MaxFun, eleveldb:iterator_move(Iterator, next), [{Key, binary_to_term(ValueNext)} | KeysValues], Decoder);
                false -> KeysValues
            end;
        {error, invalid_iterator}   -> KeysValues
    end.

encode_key(Key, undefined) when is_binary(Key) -> Key;
encode_key(Key, undefined)					   -> key_not_binary(Key);
encode_key(Key, Fun) ->
	KeyEncoded = Fun(Key),
	case is_binary(KeyEncoded) of
		true  -> KeyEncoded;
		false -> key_not_binary(Key)
	end.

prefix_key(Key, Prefix) ->
	<<Prefix/binary, ?SEPARATOR/binary, Key/binary>>.

expand_key(BucketAndKey) ->
    case binary:split(BucketAndKey, ?SEPARATOR) of
        [Bucket, Key]  -> {Bucket, Key};
        [BucketAndKey] -> {undefined, BucketAndKey}
    end.

key_not_binary(Key) ->
	throw({key_must_be_binary, Key}).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

store_test_() ->
    [{foreach, local,
		fun test_setup/0,
      	fun test_teardown/1,
      [
        {"prefix and extract key", fun test_prefix_key/0},
      	{"put data", fun test_put/0},
        {"put data with key encoding", fun test_put_encode/0},
      	{"put and delete data", fun test_delete/0},
        {"get range", fun test_range/0},
        {"get range with encoding", fun test_range_with_encoding/0},
        {"get range with fun", fun test_range_fun/0},
        {"get next key", fun test_next/0},
      	{"use commands", fun test_commands/0}
		]}
	].

test_setup() ->
	os:cmd("rm -rf " ++ ?TESTDB).
 
test_teardown(_) ->
	nothing.

test_prefix_key()  ->
    ?assertEqual({<<"bucket">>, <<"key">>}, expand_key(prefix_key(<<"key">>, <<"bucket">>))),
    ?assertEqual({undefined, <<"solitary_key">>}, expand_key(<<"solitary_key">>)).

test_put() ->
    Bucket = <<"bucket">>,
    Handle = handle(?TESTDB, [{create_if_missing, true}]),
    ?assertException(
    	throw,
    	{key_must_be_binary, key},
    	?MODULE:put(Bucket, key, value, Handle, [{put_options, [sync, true]}])
    ),
    ?assertException(
    	throw,
    	{key_must_be_binary, key},
    	?MODULE:get(Bucket, key, Handle, [])
    ),
    ?assertEqual(not_found, ?MODULE:get(Bucket, <<"nonexisting_key">>, Handle, [])),
    ?MODULE:put(Bucket, <<"key">>, value, Handle, [{put_options, [sync, true]}]),
    ?MODULE:put(<<"another_bucket">>, <<"another_key">>, value, Handle, [{put_options, [sync, true]}]),
    ?assertEqual(not_found, ?MODULE:get(Bucket, <<"another_key">>, Handle, [])),
    ?assertEqual({<<"key">>, value}, ?MODULE:get(Bucket, <<"key">>, Handle, [])).

test_put_encode() ->
    Bucket = <<"bucket">>,
    Handle = handle(?TESTDB, [{create_if_missing, true}]),
    ?assertException(
    	throw,
    	{key_must_be_binary, min},
    	?MODULE:range(Bucket, min, max, Handle, [{key_encoder, fun erlang:atom_to_list/1}, {key_decoder, fun erlang:list_to_atom/1}])
    ),
    ?assertException(
    	throw,
    	{key_must_be_binary, key},
    	?MODULE:get(Bucket, key, Handle, [{key_encoder, fun(K) -> K end}])
    ),
    ?assertEqual(not_found, ?MODULE:get(Bucket, nonexisting_key, Handle, [{key_encoder, fun erlang:term_to_binary/1}])),
    ?assertEqual(ok, ?MODULE:put(Bucket, key, value, Handle, [{key_encoder, fun erlang:term_to_binary/1}])),
	?assertEqual({key, value}, ?MODULE:get(Bucket, key, Handle, [{key_encoder, fun erlang:term_to_binary/1}])).

test_delete() ->
    Bucket = <<"bucket">>,
    Handle = handle(?TESTDB, [{create_if_missing, true}]),
    ?MODULE:put(Bucket, <<"key">>, value, Handle, [{put_options, [sync, true]}]),
    ?assertEqual({<<"key">>, value}, ?MODULE:get(Bucket, <<"key">>, Handle, [])),    
    delete(Bucket, <<"key">>, Handle, [{sync, true}]),
    ?assertEqual(not_found, ?MODULE:get(Bucket, <<"key">>, Handle, [])).

test_range() ->
    Bucket = <<"bucket">>,
    Handle = handle(?TESTDB, [{create_if_missing, true}]),
    ?MODULE:put(<<"another_bucket">>, <<"aa">>, value, Handle, [{put_options, [sync, true]}]),
    Put = fun(Key, Value) -> ?MODULE:put(Bucket, Key, Value, Handle, [{put_options, [sync, true]}]) end,
    [Put(K, V) || {K, V} <- [{<<"a">>, a}, {<<"b">>, b}, {<<"c">>, c}, {<<"d">>, d}, {<<"e">>, e}]],
    KeysEval = fun(Min, Max) -> range(Bucket, Min, Max, Handle) end,
    ?assertEqual([{<<"a">>, a}], KeysEval(<<"a">>, <<"a">>)),
    ?assertEqual([{<<"a">>, a}], KeysEval(<<"">>, <<"a">>)),
    ?assertEqual([{<<"a">>, a}, {<<"b">>, b}, {<<"c">>, c}], KeysEval(<<"">>, <<"c">>)),
    ?assertEqual([{<<"c">>, c}, {<<"d">>, d}, {<<"e">>, e}], KeysEval(<<"c">>, <<"x">>)),
    ?assertEqual([], KeysEval(<<"x">>, <<"y">>)),
    ?assertEqual([], KeysEval(<<"">>, <<"">>)),
    ?assertEqual([], KeysEval(<<"a">>, <<"">>)).

test_range_with_encoding() ->
    Bucket = <<"bucket">>,
    Handle = handle(?TESTDB, [{create_if_missing, true}]),
    ?assertException(
        throw,
        {key_must_be_binary, min},
        ?MODULE:range(Bucket, min, max, Handle, [{key_encoder, fun erlang:atom_to_list/1}, {key_decoder, fun erlang:list_to_atom/1}])
    ),
    Put = fun(Key, Value) -> ?MODULE:put(Bucket, Key, Value, Handle, [{put_options, [sync, true]}, {key_encoder, fun erlang:term_to_binary/1}]) end,
    [Put(K, V) || {K, V} <- [{a, "a"}, {b, "b"}, {c, "c"}, {d, "d"}, {e, "e"}]],
    EncoderDecoderOptions = [{key_encoder, fun erlang:term_to_binary/1}, {key_decoder, fun erlang:binary_to_term/1}],
    KeysEval = fun(Min, Max) -> range(Bucket, Min, Max, Handle, EncoderDecoderOptions) end,
    ?assertEqual([{a, "a"}], KeysEval(a, a)),
    ?assertEqual([{a, "a"}], KeysEval('', a)),
    ?assertEqual([{a, "a"}, {b, "b"}, {c, "c"}], KeysEval('', c)),
    ?assertEqual([{c, "c"}, {d, "d"}, {e, "e"}], KeysEval(c, x)),
    ?assertEqual([], KeysEval(x, y)),
    ?assertEqual([], KeysEval('', '')),
    ?assertEqual([], KeysEval(a, '')),
    ?assertEqual([], KeysEval(a, fun(_) -> false end)),
    ?assertEqual([{c, "c"}, {d, "d"}, {e, "e"}], KeysEval(c, fun(_) -> true end)),
    ?assertEqual([{c, "c"}, {d, "d"}], KeysEval(c, fun(Key) -> Key < e end)),
    ?assertEqual([], KeysEval(c, fun(Key) -> Key < a end)).


test_range_fun() ->
    Bucket = <<"bucket">>,
    Handle = handle(?TESTDB, [{create_if_missing, true}]),
    ?MODULE:put(Bucket, <<"a">>, a, Handle, [{put_options, [sync, true]}]),
    ?MODULE:put(Bucket, <<"b">>, b, Handle, [{put_options, [sync, true]}]),
    ?MODULE:put(Bucket, <<"c">>, c, Handle, [{put_options, [sync, true]}]),
    ?MODULE:put(Bucket, <<"d">>, d, Handle, [{put_options, [sync, true]}]),
    ?MODULE:put(Bucket, <<"e">>, e, Handle, [{put_options, [sync, true]}]),
    ?assertEqual([], range(Bucket, <<"a">>, fun(_) -> false end, Handle)),
    ?assertEqual([{<<"c">>, c}, {<<"d">>, d}, {<<"e">>, e}], range(Bucket, <<"c">>, fun(_) -> true end, Handle)),
    ?assertEqual([{<<"c">>, c}, {<<"d">>, d}], range(Bucket, <<"c">>, fun(Key) -> Key < <<"e">> end, Handle)),
    ?assertEqual([], range(Bucket, <<"c">>, fun(Key) -> Key < <<"a">> end, Handle)).

test_next() ->
    Bucket1 = <<"bucket1">>,
    Bucket2 = <<"bucket2">>,
    Handle = handle(?TESTDB, [{create_if_missing, true}]),
    ?MODULE:put(Bucket1, <<"key1">>, value1, Handle, [{put_options, [sync, true]}]),
    ?MODULE:put(Bucket1, <<"key2">>, value2, Handle, [{put_options, [sync, true]}]),
    ?MODULE:put(Bucket1, <<"key3">>, value3, Handle, [{put_options, [sync, true]}]),
    ?MODULE:put(Bucket2, <<"key1">>, value1, Handle, [{put_options, [sync, true]}]),
    ?assertEqual({<<"key1">>, value1}, next(Bucket1, <<>>, Handle)),
    ?assertEqual({<<"key1">>, value1}, next(Bucket1, <<"key1">>, Handle)),
    ?assertEqual({<<"key2">>, value2}, next_larger(Bucket1, <<"key1">>, Handle)),
    ?assertEqual({<<"key3">>, value3}, next_larger(Bucket1, <<"key2">>, Handle)),
    ?assertEqual(not_found, next_larger(Bucket1, <<"key3">>, Handle)),
    ?assertEqual({<<"key1">>, value1}, next_larger(Bucket2, <<"key">>, Handle)),
    ?assertEqual({<<"key1">>, value1}, next(Bucket2, <<"key">>, Handle)).

test_commands() ->
    Handle = handle(?TESTDB, [{create_if_missing, true}]),
    PutCommand = put_command(<<"bucket">>, <<"key">>, value),
    write([PutCommand], Handle, [{sync, true}]),
    ?assertEqual({<<"key">>, value}, ?MODULE:get(<<"bucket">>, <<"key">>, Handle, [])),
    DeleteCommand = delete_command(<<"bucket">>, <<"key">>),
    write([DeleteCommand], Handle, [{sync, true}]),
    ?assertEqual(not_found, ?MODULE:get(<<"bucket">>, <<"key">>, Handle, [])).



-endif.
