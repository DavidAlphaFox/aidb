-module(ai_postgres_returning_builder).
-include("ai_db_query.hrl").
-export([build/1]).
build(#ai_db_query_context{query = Query} = Ctx)->
  build(Query,Ctx).
build(#ai_db_query{returning = undefined},Ctx) -> Ctx;
build(#ai_db_query{returning = []},Ctx)-> Ctx;
build(#ai_db_query{table = Table,returning = Returning},
      #ai_db_query_context{ sql = Sql } = Ctx)->
  Table1 =
    case Table of
      {as,Table0,_} -> Table0;
       _ -> Table
    end,
  Fields =
    lists:map(
      fun(F)-> ai_postgres_escape:escape_field({Table1,F})
      end,Returning),
  FieldsCSV = ai_string:join(Fields, <<" , ">>),
  Ctx#ai_db_query_context{
    sql = <<Sql/binary," RETURNING ", FieldsCSV/binary>>
   }.
