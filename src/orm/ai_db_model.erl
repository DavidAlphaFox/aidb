 -module(ai_db_model).
-export([
         name/1,
         module/1,
         fields/1,
         new/1,
         new/2,
         get_field/2,
         set_field/3
        ]).
-export([wakeup/1,sleep/2]).
-export([build/2,build/3]).
-compile({inline,[build_fields/4]}).

-spec build(map(),map()) -> map().
build(Schema,Input)->
  Fields = ai_db_schema:fields(Schema),
  ModelName = ai_db_schema:name(Schema),
  ModelFields =
    lists:foldl(
      fun(#{name := Key},Acc)->
          KeyBin = ai_string:to_string(Key),
          build_fields(Key,KeyBin,Input,Acc)
      end,#{},Fields),
  new(ModelName,ModelFields).

-spec build(map(),[atom()],map()) -> map().
build(Schema,Emnu,Input)->
  Fields = ai_db_schema:fields(Schema),
  ModelName = ai_db_schema:name(Schema),
  ModelFields =
    lists:foldl(
      fun(#{name := Key},Acc)->
          case lists:member(Key,Emnu) of
            true ->
              KeyBin = ai_string:to_string(Key),
              build_fields(Key,KeyBin,Input,Acc);
            _ -> Acc
          end
      end,#{},Fields),
  new(ModelName,ModelFields).

build_fields(Key,KeyBin,Input,Acc)->
  case maps:get(KeyBin,Input,undefined) of
    undefined -> Acc;
    Value -> Acc#{Key => Value}
  end.

-spec name(map()) -> atom().
name(Model) -> maps:get(name, Model, undefined).
-spec module(map()) -> atom().
module(Model) -> maps:get(module, Model, undefined).
-spec fields(map()) -> map().
fields(Model) -> maps:get(fields, Model, #{}).

-spec get_field(atom(),map()) -> term().
get_field(Name,Model) -> maps:get(Name,fields(Model), undefined).

-spec set_field(atom(),term(),map()) -> map().
set_field(FieldName, FieldValue, Model = #{fields := Fields}) ->
  maps:put(fields, maps:put(FieldName, FieldValue, Fields), Model);
set_field(FieldName, FieldValue, Fields) ->
  maps:put(FieldName, FieldValue, Fields).

-spec new(atom()) -> map().
new(ModelName) -> new(ModelName, #{}).

-spec new(atom(),term()) -> map().
new(ModelName, Fields) ->
  Module = module_attr(ModelName),
  #{name => ModelName, module => Module, fields => Fields}.

-spec wakeup(map()) -> term().
wakeup(Model) ->
  Module = module(Model),
  Fields = fields(Model),
  Module:wakeup(Fields).

-spec sleep(atom(),term()) -> map().
sleep(Name,Entity)->
  Module = module_attr(Name),
  Fields = Module:sleep(Entity),
  #{name => Name, module => Module, fields => Fields}.
%%%===================================================================
%%% Internal functions
%%%===================================================================
module_attr(ModelName)->
  Key = {ModelName,module},
  Fun = fun() ->
            ai_db_manager:attr_value(ModelName, module)
        end,
  ai_process:get(Key,Fun).
