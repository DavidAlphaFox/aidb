-module(ai_db_query).
-include("ai_db_query.hrl").
-compile({inline,[do_prefix/2]}).
-export([cols/1,prefix/2,prefix/3]).

-export([
         new/0,
         select/3,
         where/2,
         having/2,
         limit/2,
         offset/2,
         join/2,
         join/4,
         join/5,
         group_by/2,
         order_by/2,
         build/1,
         build/2,
         build/3
        ]).

-spec prefix(Prefix::binary()|atom(),
            Fields::map())->list().
prefix(Prefix,Fields)->
  prefix(Prefix,Fields,undefined).

prefix(Prefix,Fields,undefined)-> do_prefix(Prefix,Fields);
prefix(Prefix,Fields,Allowed)->
  FilterdFields = maps:with(Allowed,Fields),
  do_prefix(Prefix,FilterdFields).

do_prefix(Prefix,Fields)->
  maps:fold(
    fun(Key,Attrs,Acc)->
        case proplists:lookup(as,Attrs) of
          {as,AsKey}-> [{as,{Prefix,Key},AsKey}|Acc];
          _ -> [{as,{Prefix,Key},Key}|Acc]
        end
    end,[],Fields).

-spec cols(Fields::map()) -> list().
cols(Fields)->
  maps:fold(
    fun(Key,Attrs,Acc)->
      case proplists:lookup(as,Attrs) of
        {as,AsKey} ->[{as,Key,AsKey}|Acc];
        _ -> [Key|Acc]
      end
    end,[],Fields).

new() -> #ai_db_query{}.
select(Table,Fields,Query)->
  Query#ai_db_query{
    action = select,
    table = Table,
    fields = Fields
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
build(Query)->
  build(Query,ai_postgres_query_builder,[]).
build(Query,Options)->
  build(Query,ai_postgres_query_builder,Options).
build(Query,Module,Options) ->
  Context = #ai_db_query_context{
               query = Query,
               options = Options,
               sql = <<"">>,
               bindings = []
              },
  Module:build(Context).
