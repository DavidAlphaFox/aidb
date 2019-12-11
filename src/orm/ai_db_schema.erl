-module(ai_db_schema).
-export([
         def_schema/2,
         def_field/3,
         schema_fields/1,
         schema_name/1,
         schema_as/1,
         schema/1,
         field_name/1,
         field_type/1,
         field_attrs/1,
         field_is/2,
         id_name/1,
         id_type/1
        ]).


def_schema(Name, Fields) ->
  S = #{name => Name, fields => Fields},
  _ = id_field(S),
  S.

def_field(Name, Type, Attributes) ->
  #{name => Name, type => Type, attrs => Attributes}.

schema_name(Schema) -> maps:get(name, Schema, undefined).
schema_fields(Schema) -> maps:get(fields, Schema, []).
schema_as(Schema)->
  Fields = maps:get(fields, Schema, []),
  lists:foldl(
    fun(F,Acc)->
        Key = field_name(F),
        Attrs = field_attrs(F),
        case lists:keyfield(as,1,Attrs) of
          false -> [Key|Acc];
          {as,ASKey} -> [{as,Key,ASKey}|Attrs]
        end
    end,[],Fields).

schema(ModelName) ->
  Module = ai_db_manager:attr_value(ModelName, module),
  Module:schema().

field_name(#{name := Name}) -> Name.
field_type(#{type := Type}) -> Type.
field_attrs(_Field = #{attrs := Attributes}) -> Attributes.
field_is(What, #{attrs := Attributes}) ->
    proplists:is_defined(What, Attributes).

id_name(Schema) -> field_name(id_field(Schema)).
id_type(Schema) -> field_type(id_field(Schema)).

%%%===================================================================
%%% Internal functions
%%%===================================================================
id_field(_Schema = #{fields := Fields}) ->
  erlang:hd(lists:filter(fun(_Field = #{attrs := Attributes}) ->
    lists:member(id, Attributes)
  end, Fields)).
