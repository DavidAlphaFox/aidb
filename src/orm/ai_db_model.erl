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
-export([build_fields/2,build_fields/3]).
-compile({inline,[
                  build_field_key/2,
                  build_fields/4,
                  schema_attrs/1
                 ]}).


build_fields(Schema,Input)->
  AttrsMap = schema_attrs(Schema),
  lists:foldl(
    fun({Key,Attrs},Acc)->
        KeyBin = build_field_key(Key,Attrs),
        build_fields(Key,KeyBin,Input,Acc)
  end,#{},AttrsMap).

build_fields(Schema,Emnu,Input)->
  AttrsMap = schema_attrs(Schema),
  lists:foldl(
    fun({Key,Attrs},Acc)->
      case lists:member(Key,Emnu) of
        true ->
          KeyBin = build_field_key(Key,Attrs),
          build_fields(Key,KeyBin,Input,Acc);
        _ -> Acc
      end
  end,#{},AttrsMap).

build_field_key(Key,Attrs) ->
  case lists:keyfind(as,1,Attrs) of
    false -> ai_string:to_string(Key);
    {as,TK} -> ai_string:to_string(TK)
  end.

build_fields(Key,KeyBin,Input,Acc)->
  case maps:get(KeyBin,Input,undefined) of
    undefined -> Acc;
    Value -> Acc#{Key => Value}
  end.


schema_attrs(Schema)->
  lists:map(fun(F) ->
                {
                 ai_db_schema:field_name(F),
                 ai_db_schema:field_attrs(F)
                }
            end,ai_db_schema:schema_fields(Schema)).


model_name(Model) -> maps:get(name, Model, undefined).
model_module(Model) -> maps:get(module, Model, undefined).
model_fields(Model) -> maps:get(fields, Model, #{}).

get_field(Name,Model) -> maps:get(Name, model_fields(Model), undefined).

set_field(FieldName, FieldValue, Model = #{fields := Fields}) ->
  maps:put(fields, maps:put(FieldName, FieldValue, Fields), Model);
set_field(FieldName, FieldValue, Fields) ->
  maps:put(FieldName, FieldValue, Fields).

new_model(Name) -> new_model(Name, #{}).

new_model(Name, Fields) ->
  Module = ai_db_manager:attr_value(Name, module),
  #{name => Name, module => Module, fields => Fields}.

wakeup(Model) ->
  Module = model_module(Model),
  Fields = model_fields(Model),
  Module:wakeup(Fields).

sleep(Name,Model)->
  Module = ai_db_manager:attr_value(Name,module),
  Fields = Module:sleep(Model),
  #{name => Name, module => Module, fields => Fields}.
