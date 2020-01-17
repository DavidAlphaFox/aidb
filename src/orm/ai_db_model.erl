 -module(ai_db_model).
-export([
         name/1,
         module/1,
         fields/1,
         new/1,
         new/2,
         get_field/2,
         set_field/3,
         is_new/1,
         is_dirty/1,
         persist/1
        ]).
-export([build/2,build/3]).
-compile({inline,[build_fields/2]}).

-spec build(atom(),map()) -> map().
build(ModelName,Input)->
  EntityFields = build_fields(ModelName, Input),
  new(ModelName,EntityFields,persist).

-spec build(atom(),[atom()],map()) -> map().
build(ModelName,Allowed,Input)->
  EntityFields = build_fields(ModelName, Input),
  FilteredEntityFields = maps:with(Allowed,EntityFields),
  new(ModelName,FilteredEntityFields,persist).

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
set_field(FieldName, FieldValue,
          Model = #{fields := Fields, attrs := Attr}) ->
  Fields0 = maps:put(FieldName, FieldValue, Fields),
  Attr0 = sets:add_element(dirty,Attr),
  Model#{fields => Fields0, attrs => Attr0};
set_field(FieldName, FieldValue, Fields) ->
  maps:put(FieldName, FieldValue, Fields).

-spec new(atom()) -> map().
new(ModelName) -> new(ModelName, #{}).

-spec new(atom(),map()) -> map().
new(ModelName, Fields) -> new(ModelName,Fields,new).

persist(#{attrs := Attr} = Model)->
  Attr0 = sets:del_element(new,Attr),
  Model#{attrs => Attr0}.

is_new(Model) -> is(new,Model).
is_dirty(Model) -> is(dirty,Model).

%%%===================================================================
%%% Internal functions
%%%===================================================================
module_attr(ModelName)->
  Key = {ModelName,module},
  Fun = fun() ->
            ai_db_manager:attr_value(ModelName, module)
        end,
  ai_process:get(Key,Fun).

is(What, #{attrs := Attributes}) -> sets:is_element(What,Attributes).

new(ModelName,Fields,NewOrPersist)->
  Module = module_attr(ModelName),
  Attrs = sets:new(),
  #{name => ModelName, module => Module, fields => Fields,
    attrs => sets:add_element(NewOrPersist,Attrs)}.
