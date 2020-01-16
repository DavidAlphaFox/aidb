-module(ai_db_schema).
-export([
         define/2,
         fields/1,
         name/1,
         schema/1,
         id_name/1,
         id_type/1
        ]).


define(Name, Fields) ->
  S = #{name => Name, fields => Fields},
  _ = id_field(S),
  S.

name(Schema) -> maps:get(name, Schema, undefined).
fields(Schema) -> maps:get(fields, Schema, []).

schema(ModelName) ->
  Key = {ModelName,schema},
  case erlang:get(Key) of
    undefined ->
      Module = module(ModelName),
      Schema = Module:schema(),
      erlang:put(Key,Schema),
      Schema;
    Cached -> Cached
  end.


id_name(Schema) -> ai_db_field:name(id_field(Schema)).
id_type(Schema) -> ai_db_field:type(id_field(Schema)).

%%%===================================================================
%%% Internal functions
%%%===================================================================
module(ModelName)->
  Key = {ModelName,module},
  case erlang:get(Key) of
    undefined ->
      M = ai_db_manager:attr_value(ModelName, module),
      erlang:put(Key,M),
      M;
    Cached -> Cached
  end.
id_field(#{name := ModelName,fields := Fields}) ->
  Key = {ModelName,id_field},
  IDField =
    case erlang:get(Key) of
      undefined ->
        lists:filter(fun(_Field = #{attrs := Attributes}) ->
                         lists:member(id, Attributes)
                     end, Fields);
      Cached -> Cached
    end,
  case IDField of
    [] -> throw({error,bad_id_field});
    [ID] ->
      erlang:put(Key,ID),
      ID;
    IDField -> IDField
  end.
