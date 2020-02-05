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
         persist/1
        ]).
-export([build/2,build/3]).
-compile({inline,[build/4]}).

-spec build(atom()|map(),map()) -> map().
build(Schema,Input) when erlang:is_map(Schema)->
  ModelName = ai_db_schema:name(Schema),
  Fields = ai_db_schema:fields(Schema),
  build(ModelName,Fields,undefined,Input);
build(ModelName,Input)->
  Schema = ai_db_schema:schema(ModelName),
  Fields = ai_db_schema:fields(Schema),
  build(ModelName,Fields,undefined,Input).

-spec build(atom()|map(),[atom()],map()) -> map().
build(Schema,Allowed,Input) when erlang:is_map(Schema)->
  ModelName = ai_db_schema:name(Schema),
  Fields = ai_db_schema:fields(Schema),
  build(ModelName,Fields,Allowed,Input);
build(ModelName,Allowed,Input)->
  Schema = ai_db_schema:schema(ModelName),
  Fields = ai_db_schema:fields(Schema),
  build(ModelName,Fields,Allowed,Input).
  
build(ModelName,Fields,Allowed,Input)->
  EntityFields = 
    lists:foldl(
      fun(#{name := Key},Acc)->
          case maps:is_key(Key,Input) of
            false ->
              KeyBin = ai_string:to_string(Key),
              case maps:is_key(KeyBin,Input) of
                false -> Acc;
                true ->
                  Value = maps:get(KeyBin,Input),
                  Acc#{Key => Value}
              end;
            true ->
              Value = maps:get(Key,Input),
              Acc#{Key => Value}
          end
      end,#{},Fields),
  FilteredEntityFields =
    if
      Allowed == undefined -> EntityFields;
      true -> maps:with(Allowed,EntityFields)
    end,
  new(ModelName,FilteredEntityFields).

  
-spec name(map()) -> atom().
name(Model) -> maps:get(name, Model, undefined).
-spec module(map()) -> atom().
module(Model) -> maps:get(module, Model, undefined).
-spec fields(map()) -> map().
fields(Model) -> maps:get(fields, Model, #{}).

-spec get_field(atom(),map()) -> term().
get_field(Name,Model) -> maps:get(Name,fields(Model), undefined).

-spec set_field(atom(),term(),map()) -> map().
set_field(FieldName, FieldValue,Model = #{fields := Fields}) ->
  Fields0 = maps:put(FieldName,FieldValue, Fields),
  Model#{fields => Fields0}.

-spec new(atom()) -> map().
new(ModelName) -> new(ModelName, #{}).

-spec new(atom(),map()) -> map().
new(ModelName, Fields) ->
  Module = module_attr(ModelName),
  #{name => ModelName, module => Module,
    fields => Fields,attrs => [new]}.

-spec persist(map()) -> map().
persist(Model)-> Model#{attrs => [persist]}.

-spec is_new(map()) -> boolean().
is_new(Model) -> is(new,Model).

%%%===================================================================
%%% Internal functions
%%%===================================================================
module_attr(ModelName)->
  CacheKey = {ai_db,ModelName,module},
  Fun = fun() ->
            ai_db_manager:attr_value(ModelName, module)
        end,
  ai_process:get(CacheKey,Fun).

is(What, #{attrs := Attributes}) -> proplists:is_defined(What,Attributes).
