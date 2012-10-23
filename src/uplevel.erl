-module(uplevel).
-author('Florian Odronitz <odo@mac.com>').

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(TESTDB, "/tmp/uplevel.test").
-endif.

-export([
	handle/1, handle/2,
    put/5, put/4,
    put_command/3,
	delete_command/2,
	get/4, get/3,
    delete/3, delete/4,
    write/2, write/3,
    range/4,
    next/3,
    next_from_iterator/3,
    next_larger/3
]).

-type table_handle() :: any().
-type put_options()  :: [{put_options, []}] | [].
-type get_options()  :: [{key_decoder, fun()}] | [].
-type delete_options()  :: [{sync, boolean()}] | [].
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

-spec put(binary(), any(), any(), table_handle()) -> ok | {error, any()}.
put(Bucket, Key, Value, Handle) ->
    put(Bucket, Key, Value, Handle, []).

-spec put(binary(), any(), any(), table_handle(), put_options()) -> ok | {error, any()}.
put(Bucket, Key, Value, Handle, Options) when is_binary(Bucket) andalso is_binary(Key) ->
    eleveldb:write(Handle, [put_command(Bucket, Key, Value)], Options).

-spec put_command(binary(), any(), any()) -> {'put', binary(), binary()}.
put_command(Bucket, Key, Value) when is_binary(Bucket) andalso is_binary(Key) ->
	KeyPrefixed = prefix_key(Key, Bucket),
    {put, KeyPrefixed, term_to_binary(Value)}.

-spec get(binary(), any(), table_handle()) -> 'not_found' | {binary(), binary()}.
get(Bucket, Key, Handle) ->
    get(Bucket, Key, Handle, []).

-spec get(binary(), any(), table_handle(), get_options()) -> 'not_found' | {binary(), binary()}.
get(Bucket, Key, Handle, Options) when is_binary(Bucket) andalso is_binary(Key) ->
	KeyPrefixed = prefix_key(Key, Bucket),
	case eleveldb:get(Handle, KeyPrefixed, proplists:get_value(get_options, Options, [])) of
		{ok, Value} -> {Key, binary_to_term(Value)};
		not_found -> not_found
	end.

-spec delete(binary(), any(), table_handle()) -> ok.
delete(Bucket, Key, Handle) ->
    delete(Bucket, Key, Handle, []).

-spec delete(binary(), any(), table_handle(), delete_options()) -> ok.
delete(Bucket, Key, Handle, Options) when is_binary(Bucket) andalso is_binary(Key) ->
    eleveldb:write(Handle, [delete_command(Bucket, Key)], Options).

-spec delete_command(binary(), any()) -> {'delete', binary()}.
delete_command(Bucket, Key) when is_binary(Bucket) andalso is_binary(Key) -> 
    KeyPrefixed = prefix_key(Key, Bucket),
    {delete, KeyPrefixed}.

-spec write([command()], any()) -> ok | {error, any()}.
write(Commands, Handle) ->
    write(Commands, Handle, []).

-spec write([command()], any(), delete_options()) -> ok | {error, any()}.
write(Commands, Handle, Options) ->
    eleveldb:write(Handle, Commands, Options).

-spec range(binary(), binary(), max_key(), table_handle()) -> [binary()].
range(Bucket, Min, Max, Handle) when is_binary(Bucket) andalso is_binary(Min) andalso is_binary(Max) ->
    {ok, Iterator} = eleveldb:iterator(Handle, []),
    KeyMinComposite = prefix_key(Min, Bucket),
    Keys =
    case Min > Max of
        true ->
            [];
        false ->
            next_key_max(Iterator, Max, eleveldb:iterator_move(Iterator, KeyMinComposite))
    end,
    eleveldb:iterator_close(Iterator),
    Keys.

% get keys until the key equals a given key
next_key_max(Iterator, Max, Candidate) ->
    case Candidate of
        {ok, CompositeKeyNext, ValueNext} ->
            {_Bucket, Key} = expand_key(CompositeKeyNext),
            case Key of
                _ when Key =< Max ->
                    [{Key, binary_to_term(ValueNext)}|next_key_max(Iterator, Max, eleveldb:iterator_move(Iterator, next))];
                _ -> []
            end;
        {error, invalid_iterator}   -> []
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

next_from_iterator(Bucket, KeyMin, Iterator) ->
    case eleveldb:iterator_move(Iterator, prefix_key(KeyMin, Bucket)) of
        {ok, CompositeKey} ->
            {BucketIn, Key} = expand_key(CompositeKey),
            case BucketIn of
                Bucket ->
                    Key;
                _ ->
                    not_found
            end;
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

prefix_key(Key, Prefix) ->
	<<Prefix/binary, ?SEPARATOR/binary, Key/binary>>.

expand_key(BucketAndKey) ->
    case binary:split(BucketAndKey, ?SEPARATOR) of
        [Bucket, Key]  -> {Bucket, Key};
        [BucketAndKey] -> {undefined, BucketAndKey}
    end.

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
        {"get next key", fun test_next/0},
        {"get next key wirth itarator", fun test_next_from_iterator/0},
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
    ?assertEqual(not_found, ?MODULE:get(Bucket, <<"nonexisting_key">>, Handle, [])),
    ?MODULE:put(Bucket, <<"key">>, value, Handle, [{put_options, [sync, true]}]),
    ?MODULE:put(<<"another_bucket">>, <<"another_key">>, value, Handle, [{put_options, [sync, true]}]),
    ?assertEqual(not_found, ?MODULE:get(Bucket, <<"another_key">>, Handle, [])),
    ?assertEqual({<<"key">>, value}, ?MODULE:get(Bucket, <<"key">>, Handle, [])).

test_put_encode() ->
    Bucket = <<"bucket">>,
    Handle = handle(?TESTDB, [{create_if_missing, true}]),
    ?assertEqual(not_found, ?MODULE:get(Bucket, <<"nonexisting_key">>, Handle)),
    ?assertEqual(ok, ?MODULE:put(Bucket, <<"key">>, value, Handle)),
	?assertEqual({<<"key">>, value}, ?MODULE:get(Bucket, <<"key">>, Handle)).

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

test_next_from_iterator() ->
    Bucket1 = <<"bucket1">>,
    Handle = handle(?TESTDB, [{create_if_missing, true}]),
    ?MODULE:put(Bucket1, <<"key1">>, value1, Handle, [{put_options, [sync, true]}]),
    {ok, Iterator1} = eleveldb:iterator(Handle, [], keys_only),
    ?assertEqual(<<"key1">>, next_from_iterator(Bucket1, <<"key">>, Iterator1)),
    {ok, Iterator2} = eleveldb:iterator(Handle, []),
    ?assertEqual({<<"key1">>, value1}, next_from_iterator(Bucket1, <<"key">>, Iterator2)).
    
test_commands() ->
    Handle = handle(?TESTDB, [{create_if_missing, true}]),
    PutCommand = put_command(<<"bucket">>, <<"key">>, value),
    write([PutCommand], Handle, [{sync, true}]),
    ?assertEqual({<<"key">>, value}, ?MODULE:get(<<"bucket">>, <<"key">>, Handle, [])),
    DeleteCommand = delete_command(<<"bucket">>, <<"key">>),
    write([DeleteCommand], Handle, [{sync, true}]),
    ?assertEqual(not_found, ?MODULE:get(<<"bucket">>, <<"key">>, Handle, [])).



-endif.
