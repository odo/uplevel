all: deps compile

compile:
	rebar compile

deps:
	rebar get-deps

clean:
	rebar clean

test:
	rebar skip_deps=true eunit

start:
	erl -pz ebin deps/*/ebin

analyze: compile
	rebar analyze skip_deps=true