-module(ai_pgsql_escape).
-export([
         slot/1,
         field/1,
         value/1
        ]).

-type column() :: atom() | binary() | string() | tuple().
-export_type([column/0]).

-spec slot(Slot::integer())->binary().
slot(Slot) ->
  N = ai_string:to_string(Slot),
  <<$\s,$$,N/binary,$\s>>.

-spec field(
        {sql,column()} |
        {sql,column(),column()} |
        {proc,column(),column()} |
        {proc,column(),column(),column()} |
        {as,column(),column()} |
        {column(),column()} |
        column()
       ) -> binary().
field({sql,Field})-> Field;
field({sql,Field,ASField})->
  AF = field(ASField),
  <<Field/binary," AS ",AF/binary>>;
field({proc,Proc,Fields})
  when erlang:is_list(Fields)->
  Fields0 =
    lists:map(
      fun(F)-> field(F) end, Fields),
  Fields1 = ai_string:join(Fields0,<<",">>),
  Proc0 = ai_string:to_string(Proc),
  <<Proc0/binary,"(",Fields1/binary,")">>;
field({proc,Proc,Field})->
  Proc0 = ai_string:to_string(Proc),
  F = field(Field),
  <<Proc0/binary, "(",F/binary,")">>;
field({proc,Proc,Field,Alias})->
  Proc0 = ai_string:to_string(Proc),
  F = field(Field),
  AF = field(Alias),
  <<Proc0/binary, "(",F/binary,") AS ",AF/binary>>;
field({as,Field,ASField})->
    F = field(Field),
    AF = field(ASField),
    <<F/binary," AS ",AF/binary>>;
field({Prefix,Column})->
  PrefixBin = field(Prefix),
  ColumnBin = field(Column),
  <<PrefixBin/binary,".",ColumnBin/binary>>;
field(Field) ->
  F = ai_string:to_string(Field),
  if
    F == <<"*">> -> F;
    true->
      F1 = re:replace(F,"\\\"","\\\\\"",[global,{return,binary}]),
      <<"\"",F1/binary,"\"">>
  end.

-spec value(term())-> binary().
value(Value)->
  F = ai_string:to_string(Value),
  case binary:match(F,<<"'">>) of
    nomatch ->  <<$',F/binary,$'>>;
    _->
      F1 = re:replace(F,"'","\\'",[global,{return,binary}]),
      <<"E'",F1/binary,"'">>
  end.
