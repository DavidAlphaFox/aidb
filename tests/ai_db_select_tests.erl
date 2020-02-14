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
                     { '==',{products,user_id},{field,{u,id}}}
                    ],Q1).
subquery1()->
  Query = ai_db_query:new(),
  Q1 = ai_db_query:select({as,users,u1},
                     [
                      {u1,[salary]}
                     ],Query),
  ai_db_query:where([
                     { '==',{u1,salary},10000}
                    ],Q1).

join(Query)-> ai_db_query:join(roles, id,role_id, Query).

where(Query)->
  Cond = [
          {'>',{u,id},1},
          {in,{r,name},[<<"admin">>,<<"superuser">>]},
          {{u,name},not_null},
          {'in',{u,salary},{subquery,subquery1()}}
          ],
  ai_db_query:where(Cond,Query).
  
run()->
  Q = select(),
  Q1 = join(Q),
  Q2 = where(Q1),
  ai_db_query:build(Q2).
