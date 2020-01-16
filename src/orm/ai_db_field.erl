-module(ai_db_field).
-export([define/3]).
-export([name/1,type/1,attrs/1,is/2]).

define(Name, Type, Attributes) ->
  #{name => Name, type => Type, attrs => Attributes}.
name(#{name := Name}) -> Name.
type(#{type := Type}) -> Type.
attrs(_Field = #{attrs := Attributes}) -> Attributes.
is(What, #{attrs := Attributes}) ->
    proplists:is_defined(What, Attributes).
