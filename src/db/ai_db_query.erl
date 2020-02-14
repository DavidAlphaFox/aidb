-module(ai_db_query).
-include("ai_db_query.hrl").

-export([
         new/0,
         select/3,
         insert/3,
         update/3,
         delete/2,
         where/2,
         having/2,
         limit/2,
         offset/2,
         join/2,
         join/4,
         join/5,
         group_by/2,
         order_by/2,
         returning/2,
         build/1,
         build/2,
         build/3
        ]).


new() -> #ai_db_query{}.
select(Table,Fields,Query)->
  Query#ai_db_query{
    action = select,
    table = Table,
    fields = Fields
   }.

insert(Table,Fields,Query)->
  Query#ai_db_query{
    action = insert,
    table = Table,
    fields = Fields
   }.
update(Table,Fields,Query)->
  Query#ai_db_query{
    action = update,
    table = Table,
    fields = Fields
   }.
delete(Table,Query)->
  Query#ai_db_query{
    action = delete,
    table = Table
   }.

where(Cond,Query) when erlang:is_tuple(Cond)->
  where([Cond],Query);
where(Cond,#ai_db_query{ where = undefined } = Query) ->
  Query#ai_db_query{
    where = Cond
   };
where(Cond,#ai_db_query{ where = OldCond} = Query) ->
  Query#ai_db_query{
    where = OldCond ++ Cond
   }.

having(Cond,Query) when erlang:is_tuple(Cond)->
  having([Cond],Query);
having(Cond,#ai_db_query{ having = undefined } = Query) ->
  Query#ai_db_query{
    having = Cond
   };
having(Cond,#ai_db_query{ having = OldCond } = Query) ->
  Query#ai_db_query{
    having = OldCond ++ Cond
   }.

limit(Limit, Query)-> Query#ai_db_query{ limit = Limit }.
offset(Offset,Query)-> Query#ai_db_query{ offset = Offset }.

join(Table,PK,FK,Query)->
  join({join,Table,PK,FK},Query).
join(Type,Table,PK,FK,Query)->
  join({Type,Table,PK,FK},Query).

join(Join,Query) when erlang:is_tuple(Join)->
  join([Join],Query);

join(Join,#ai_db_query{ join = undefined} = Query)->
  Query#ai_db_query{
    join = Join
   };
join(Join,#ai_db_query{ join = OldJoin } = Query) ->
  Query#ai_db_query{
    join = OldJoin ++ Join
   }.
group_by(Field,Query) when erlang:is_atom(Field)->
  group_by([Field],Query);
group_by(Fields,#ai_db_query{ group_by = undefined } = Query) ->
  Query#ai_db_query{ group_by = Fields };
group_by(Fields,#ai_db_query{ group_by = OldGroupBy } = Query) ->
  Query#ai_db_query{
    group_by = OldGroupBy ++ Fields
   }.
order_by(Field,Query) when erlang:is_tuple(Field) ->
  order_by([Field],Query);
order_by(Fields,#ai_db_query{ order_by = undefined } = Query) ->
  Query#ai_db_query{ order_by = Fields };
order_by(Fields,#ai_db_query{ order_by = OldOrderBy } = Query) ->
  Query#ai_db_query{
    order_by = OldOrderBy ++ Fields
   }.

returning(Field,Query) when erlang:is_atom(Field)->
  returning([Field],Query);
returning(Fields,Query) ->
  Query#ai_db_query{
    returning = Fields
   }.

build(Query)->
  build(Query,ai_postgres_query_builder,[]).
build(Query,Options)->
  build(Query,ai_postgres_query_builder,Options).
build(Query,Module,Options) ->
  Context = #ai_db_query_context{
               query = Query,
               options = Options,
               sql = <<"">>,
               bindings = [],
               slot = 1
              },
  Context0 = Module:build(Context),
  {Context0#ai_db_query_context.sql,Context0#ai_db_query_context.bindings}.
