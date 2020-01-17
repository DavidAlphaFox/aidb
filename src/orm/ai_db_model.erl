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
-compile({inline,[build_fields/2]}).

-spec build(atom(),map()) -> map().
build(ModelName,Input)->
  EntityFields = build_fields(ModelName, Input),
  new(ModelName,EntityFields).

-spec build(atom(),[atom()],map()) -> map().
build(ModelName,Allowed,Input)->
  EntityFields = build_fields(ModelName, Input),
  FilteredEntityFields = maps:with(Allowed,EntityFields),
  new(ModelName,FilteredEntityFields).

build_fields(ModelName,Input)->
  Schema = ai_db_schema:schema(ModelName),
  Fields = ai_db_schema:fields(Schema),
  lists:foldl(
    fun(#{name := Key},Acc)->
        case maps:get(Key,Input,undefined) of
          undefined ->
            KeyBin = ai_string:to_string(Key),
            case maps:get(KeyBin,Input,undefined) of
              undefined -> Acc;
              Value -> Acc#{Key => Value}
            end;
          Value -> Acc#{Key => Value}
        end
    end,#{},Fields).

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

-spec new(atom(),map()) -> map().
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
-spec module_attr(atom()) -> atom().
module_attr(ModelName)->
  Key = {ModelName,module},
  Fun = fun() ->
            ai_db_manager:attr_value(ModelName, module)
        end,
  ai_process:get(Key,Fun).
