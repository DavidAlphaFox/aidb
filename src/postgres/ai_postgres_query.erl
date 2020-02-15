-module(ai_postgres_query).
-include("ai_db_query.hrl").
-export([build/1]).

-export([select/3,select/4,select/6,select/7]).
-export([count/1,count/2,count/3]).
-export([insert/2,update/3,delete/2]).

-spec delete(Table::atom() | binary(),
             Conditions::list()) -> tuple().
delete(Table, Conditions) ->
  ai_lists:run_pipe(
    [
     {ai_db_query,delete,[Table]},
     {ai_db_query,where,[Conditions]},
     {ai_db_query,build,[]}
    ],[ai_db_query:new()]).

-spec update(Table::atom() | binary(), Columns::map() | list(),
            Conditions::list() ) -> tuple().
update(Table,Columns,Conditions)->
  ai_lists:run_pipe(
    [
     {ai_db_query,update,[Table,Columns]},
     {ai_db_query,where,[Conditions]},
     {ai_db_query,returning,['*']},
     {ai_db_query,build,[]}
    ],[ai_db_query:new()]).
  

-spec insert(Table::atom() | binary(),
             Columns::map() | list()) -> tuple().
insert(Table,Columns) ->
  ai_lists:run_pipe(
    [
     {ai_db_query,insert,[Table,Columns]},
     {ai_db_query,returning,['*']},
     {ai_db_query,build,[]}
    ],[ai_db_query:new()]).
    
-spec count(Table::atom() | binary()) -> tuple().
count(Table) -> count(Table,[],undefined).
-spec count(Table::atom() | binary(),
           Conditions::list()) -> tuple().
count(Table,Conditions) -> count(Table,Conditions,undefined).
-spec count(Table::atom() | binary(),Conditions::list(),
           ExtraWhere::binary()|undefined) -> tuple().
count(Table,Conditions,ExtraWhere) ->
  ai_lists:run_pipe(
    [
     {ai_db_query,select,[Table,[{proc,count,'*'}]]},
     {ai_db_query,where,[Conditions,ExtraWhere]},
     {ai_db_query,build,[]}
    ],[ai_db_query:new()]).
    
-spec select(Table::atom() | binary(),SelectColumns::list(),
             Conditions::list()) -> tuple().
select(Table,SelectColumns,Conditions)->
    select(Table,SelectColumns,Conditions,undefined,undefined,0,0).

-spec select(Table::atom() | binary(),SelectColumns::list(),
             Conditions::list(),ExtraWhere::binary() | undefiend) -> tuple().
select(Table,SelectColumns,Conditions,ExtraWhere)->
    select(Table,SelectColumns,Conditions,ExtraWhere,undefined,0,0).

-spec select(Table::atom() | binary(),SelectColumns::list(),
             Conditions::list(),OrderBy::list(),
             Limit::integer(),Offset::integer()) -> tuple().
select(Table,SelectColumns,Conditions,OrderBy,Limit,Offset)->
    select(Table,SelectColumns,Conditions,undefined,OrderBy,Limit,Offset).

-spec select(Table::atom() | binary(),SelectColumns::list(),
             Conditions::list(),ExtraWhere::binary() | undefiend,
             OrderBy::list(),Limit::integer(),Offset::integer()) -> tuple().
select(Table, SelectColumns,Conditions, ExtraWhere,
       OrderBy, Limit, Offset) ->
  ai_lists:run_pipe(
    [
     {ai_db_query,select,[Table,SelectColumns]},
     {ai_db_query,where,[Conditions,ExtraWhere]},
     {ai_db_query,order_by,[OrderBy]},
     {ai_db_query,limit,[Limit]},
     {ai_db_query,offset,[Offset]},
     {ai_db_query,build,[]}
    ],[ai_db_query:new()]).



build(#ai_db_query_context{query = Query} = Ctx)->
  build(Query#ai_db_query.action,Ctx).

build(select,Ctx)-> ai_postgres_select_builder:build(Ctx);
build(insert,Ctx)-> ai_postgres_insert_builder:build(Ctx);
build(delete,Ctx)-> ai_postgres_delete_builder:build(Ctx);
build(update,Ctx) -> ai_postgres_update_builder:build(Ctx).
