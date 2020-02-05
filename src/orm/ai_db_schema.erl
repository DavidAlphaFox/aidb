-module(ai_db_schema).
-export([
         define/2,
         fields/1,
         name/1,
         schema/1,
         id/1,
         id/2
        ]).


define(Name, Fields) ->
  S = #{name => Name, fields => Fields},
  _ = id(S),
  S.

name(Schema) -> maps:get(name, Schema, undefined).
fields(Schema) -> maps:get(fields, Schema, []).

schema(ModelName) ->
  CacheKey = {ai_db,ModelName,schema},
  Fun = fun() ->
            Module = module_attr(ModelName),
            Module:schema()
        end,
  ai_process:get(CacheKey,Fun).
  
id(#{name := ModelName,fields := Fields}) ->
  CacheKey = {ai_db,ModelName,id_field},
  Fun = fun() ->
            case
              lists:filter(fun(_Field = #{attrs := Attributes}) ->
                               lists:member(id, Attributes)
                           end,Fields) of
              [] -> error(missing_id_field);
              [IDField] -> IDField
            end
        end,
  ai_process:get(CacheKey,Fun).

id(Schema,name)->
  Field = id(Schema),
  ai_db_field:name(Field);
id(Schema,type) ->
  Field = id(Schema),
  ai_db_field:type(Field).


%%%===================================================================
%%% Internal functions
%%%===================================================================
module_attr(ModelName)->
  CacheKey = {ai_db,ModelName,module},
  Fun = fun() ->
            ai_db_manager:attr_value(ModelName, module)
        end,
  ai_process:get(CacheKey,Fun).
