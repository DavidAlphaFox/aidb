-module(ai_db_model).
-export([
         model_name/1,
         model_module/1,
         model_fields/1,
         new_model/1,
         new_model/2,
         get_field/2,
         set_field/3
        ]).
-export([wakeup/1,sleep/2]).
model_name(Model) ->
    maps:get(name, Model, undefined).
model_module(Model) ->
    maps:get(module, Model, undefined).
model_fields(Model) ->
    maps:get(fields, Model, #{}).


get_field(Name,Model) ->
    maps:get(Name, model_fields(Model), undefined).

set_field(FieldName, FieldValue, Model = #{fields := Fields}) ->
  maps:put(fields, maps:put(FieldName, FieldValue, Fields), Model);
set_field(FieldName, FieldValue, Fields) ->
  maps:put(FieldName, FieldValue, Fields).

new_model(Name) ->
    new_model(Name, #{}).

new_model(Name, Fields) ->
  Module = ai_db_manager:attr_value(Name, module),
  #{name => Name, module => Module, fields => Fields}.

wakeup(Model) ->
  Module = model_module(Model),
  Fields = model_fields(Model),
  Module:wakup(Fields).

sleep(Name,Model)->
  Module = ai_db_manager:attr_value(Name,module),
  Fields = Module:sleep(Model),
  #{name => Name, module => Module, fields => Fields}.
