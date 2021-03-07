-module(ai_pgsql_orm).
-export([select/2,
         select/3,
         select/4,
         select/5]).
select(Module,Options) -> select(Module,Options,first,undefined,undefined).
select(Module,Options,Type)-> select(Module,Options,Type,undefined,undefined).
select(Module,Options,Type,Cond)-> select(Module,Options,Type,Cond,undefined).

select(Module,Options,Type,Cond,Order)->
  Columns = case lists:keyfind(as, 1, Options) of
              {as,true} -> ai_pgsql:build_select(erlang:apply(Module, '-alias_columns', []));
             _-> ai_pgsql:build_select(erlang:apply(Module, '-columns', []))
            end,
  TableName = ai_pgsql_escape:field(ai_string:to_string(erlang:apply(Module, '-table',[]))),
  SQL0 = <<"SELECT ",Columns/binary," FROM ",TableName/binary>>,
  {CondSQL,CondValues,Next} =
    case Cond of
      undefined -> {undefined,[],1};
      _ -> ai_pgsql:build_cond(1,Cond)
    end,
  SQL1 =
    if CondSQL == undefined -> SQL0;
       true -> <<SQL0/binary, " WHERE ",CondSQL/binary>>
    end,
  SQL2 =
    case Order of
      undefined -> SQL1;
      _ ->
        [OrderH|OrderT] = Order,
        OrderSQL = lists:foldl(
                     fun(OrderCol,Acc)->
                         OrderCol0 = order_col(OrderCol),
                         <<Acc/binary,",",OrderCol0/binary>>
                     end,order_col(OrderH),OrderT),
        <<SQL1/binary," ORDER BY ",OrderSQL>>
    end,
  {SQL3,Values} = case Type of
                    first -> {<<SQL2/binary," LIMIT 1">>,CondValues};
                    Type when is_integer(Type)->
                      Slot = ai_pgsql_escape:slot(Next),
                      {<<SQL2/binary," LIMIT ",Slot/binary>>,CondValues ++ [Type]};
                    {Limit,Offset} ->
                      LSlot = ai_pgsql_escape:slot(Next),
                      OSlot = ai_pgsql_escape:slot(Next + 1),
                      {<<SQL2/binary," LIMIT ",LSlot/binary," OFFSET ",OSlot/binary>>,CondValues ++ [Limit,Offset]};
                    all -> {SQL2,CondValues}
                  end,
  fun(Conn)-> epgsql:equery(Conn,SQL3,Values) end.

order_col({Col,asc})->
  Col0 = ai_pgsql_escape:field(ai_string:to_string(Col)),
  <<Col0/binary," ASC">>;
order_col({Col,desc}) ->
  Col0 = ai_pgsql_escape:field(ai_string:to_string(Col)),
  <<Col0/binary," DESC">>;
order_col(Col)-> ai_pgsql_escape:field(ai_string:to_string(Col)).
