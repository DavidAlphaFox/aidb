-module(ai_postgres_escape).
-export([
         slot_numbered/1,
         escape_field/1,
         escape_value/1,
         escape_operator/1
        ]).

-spec slot_numbered({Prefix::char(),N::iodata()})->binary().
slot_numbered({Prefix, N}) ->
    P = ai_string:to_string(Prefix),
    H = ai_string:to_string(N),
    <<$\s,P/binary,H/binary,$\s>>.

escape_field({sql,Field})-> Field;
escape_field({sql,Field,ASField})->
  AF = escape_field(ASField),
  <<Field/binary," AS ",AF/binary>>;
escape_field({proc,Proc,Field})->
  Proc0 = ai_string:to_string(Proc),
  F = escape_field(Field),
  <<Proc0/binary, "(",F/binary,")">>;
escape_field({proc,Proc,Field,Alias})->
  Proc0 = ai_string:to_string(Proc),
  F = escape_field(Field),
  AF = escape_field(Alias),
  <<Proc0/binary, "(",F/binary,") AS ",AF/binary>>;

escape_field({as,Field,ASField})->
    F = escape_field(Field),
    AF = escape_field(ASField),
    <<F/binary," AS ",AF/binary>>;
escape_field({Prefix,Column})->
  PrefixBin = escape_field(Prefix),
  ColumnBin = escape_field(Column),
  <<PrefixBin/binary,".",ColumnBin/binary>>;
escape_field(Field) ->
  F = ai_string:to_string(Field),
  if
    F == <<"*">> -> F;
    true->
      F1 = re:replace(F,"\"","\\\"",[global,{return,binary}]),
      <<"\"",F1/binary,"\"">>
  end.

escape_value(Value)->
  F = ai_string:to_string(Value),
  case binary:match(F,<<"'">>) of
    nomatch ->  <<$',F/binary,$'>>;
    _->
      F1 = re:replace(F,"'","\\'",[global,{return,binary}]),
      <<"E'",F1/binary,"'">>
  end.

escape_operator('=<') -> <<" <= ">>;
escape_operator('/=') -> <<" <> ">>;
escape_operator('==') -> <<" = ">>;
escape_operator(Op) ->
    OpBin = ai_string:to_string(Op),
    <<$\s,OpBin/binary,$\s>>.
