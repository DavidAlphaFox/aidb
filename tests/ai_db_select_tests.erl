-module(ai_db_select_tests).
-compile([export_all]).

select()->
  Query = ai_db_query:new(),
  ai_db_query:select({as,users,u},
                     [
                      {u,[name,id,role_id]},
                      {r,[name,id]},
                      {subquery,subquery(),p}
                     ],Query).
subquery()->
  Query = ai_db_query:new(),
  Q1 = ai_db_query:select(products,
                     [
                      {products,[amount,date]}
                     ],Query),
  ai_db_query:where([
                     { '==',{prefix,products,user_id},{field,{prefix,u,id}}}
                    ],Q1).
join(Query)-> ai_db_query:join(roles, id,role_id, Query).

where(Query)->
  Cond = [
          {'>',{prefix,u,id},1},
          {in,{prefix,r,name},[<<"admin">>,<<"superuser">>]},
          {{prefix,u,name},not_null}
          ],
  ai_db_query:where(Cond,Query).
  
run()->
  Q = select(),
  Q1 = join(Q),
  Q2 = where(Q1),
  ai_db_query:build(Q2).
