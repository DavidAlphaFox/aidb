-module(ai_db_select_tests).
-compile([export_all]).

select()->
  Query = ai_db_query:new(),
  ai_db_query:select({as,users,u},
                     [
                      {u,[name,id,role_id]},
                      {r,[name,id]}
                     ],Query).

join(Query)-> ai_db_query:join({as,roles,r}, id,role_id, Query).

where(Query)->
  Cond = [
          {'>',{u,id},1},
          {in,{r,name},[<<"admin">>,<<"superuser">>]},
          {{u,name},not_null}
          ],
  ai_db_query:where(Cond,Query).

limit(Query)->
  Q1 = ai_db_query:limit(10,Query),
  ai_db_query:offset(10,Q1).
group_by(Query)-> ai_db_query:group_by([id],Query).
order_by(Query)-> ai_db_query:order_by([{id,desc}],Query).
run()->
  Q = select(),
  Q1 = join(Q),
  Q2 = where(Q1),
  Q3 = limit(Q2),
  Q4 = group_by(Q3),
  Q5 = order_by(Q4),
  ai_db_query:build(Q5).
