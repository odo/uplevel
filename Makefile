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

xref: compile
	rebar xref skip_deps=true

analyze: compile
	dialyzer ebin/*.beam deps/eleveldb/ebin/*.beam