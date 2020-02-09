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

escape_field({raw,Field})-> Field;
escape_field({raw_as,Field,ASField})->
    AF = escape_field(ASField),
    <<Field/binary," AS ",AF/binary>>;
escape_field({as,Field,ASField})->
    F = escape_field(Field),
    AF = escape_field(ASField),
    <<F/binary," AS ",AF/binary>>;
escape_field({Prefix,Column})->
    PrefixBin = ai_string:to_string(Prefix),
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
        nomatch ->  <<"",F,"'">>;
        _->
            F1 = re:replace(F,"'","\\'",[global,{return,binary}]),
            <<"E'",F1,"'">>
    end.

escape_operator('=<') -> <<" <= ">>;
escape_operator('/=') -> <<" != ">>;
escape_operator('==') -> <<" = ">>;
escape_operator(Op) ->
    OpBin = ai_string:to_string(Op),
    <<$\s,OpBin/binary,$\s>>.
