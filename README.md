uplevel
====
uplevel  is a small library using [eleveldb](https://github.com/basho/eleveldb).

It gives you:

* bucket/key access
* range queries

example:

<pre><code class="erlang">
1> Handle = uplevel:handle("/tmp/uplevel_demo").
<<>>
2>  uplevel:put(<<"animals">>, <<"colibri">>, [{color, "iridescent"}], Handle, []).
ok
3> uplevel:get(<<"animals">>, <<"colibri">>, Handle, []).
{<<"colibri">>,[{color,"iridescent"}]}
4> uplevel:put(<<"animals">>, <<"rino">>, [{color, "black"}], Handle, []).        
ok
5>  uplevel:put(<<"animals">>, <<"tapir">>, [{color, "striped"}], Handle, []).
ok
6>  uplevel:range(<<"animals">>, <<"ape">>, <<"zebra">>, Handle).
[{<<"colibri">>,[{color,"iridescent"}]},
 {<<"rino">>,[{color,"black"}]},
 {<<"tapir">>,[{color,"striped"}]}]
7> uplevel:range(<<"animals">>, <<"rino">>, <<"snake">>, Handle).    
[{<<"rino">>,[{color,"black"}]}]
</code></pre>


Enjoy.