-module(ai_db_row).
-compile({inline,
          [
           read_key/2,
           read_key/3,
           cast_value/2
          ]}).

-export([build/2,build/3]).
-export([columns/1,columns/2]).
-export([prefix/2,prefix/3]).

-spec build(Fields::map(),Input::map())-> map().
build(Fields,Input)-> build(Fields,Input,undefined).

-spec build(Fields::map(),Input::map(),
            Allowed::list()|undefined) -> map().
build(Fields,Input,Allowed)->
  FilterdFields =
    if
      Allowed == undefined -> Fields;
      true -> maps:with(Allowed,Fields)
    end,
  maps:fold(
    fun(Key,Attrs,Acc)->
        case read_key(Key,Attrs,Input) of
          {ok,Value} ->
            CastValue = cast_value(Attrs,Value),
            Acc#{Key => CastValue};
          _ -> Acc
        end
    end,#{},FilterdFields).

-spec columns(Fields::map()) -> list().
columns(Fields)-> columns(Fields,false).

-spec columns(Fields::map(),UseAs::boolean()) -> list().
columns(Fields,false) -> maps:keys(Fields);
columns(Fields,true)->
  maps:fold(
    fun(Key,Attrs,Acc)->
      case proplists:lookup(as,Attrs) of
        {as,AsKey} ->[{as,Key,AsKey}|Acc];
        _ -> [Key|Acc]
      end
    end,[],Fields).

-spec prefix(Prefix::atom(),
             Fields::map())->list().
prefix(Prefix,Fields)->
  prefix(Prefix,Fields,[]).

-spec prefix(Prefix::atom(),Fields::map(),
            Opts::list())->list().
prefix(Prefix,Fields,Opts)->
  UseAs = proplists:get_value(as,Opts,false),
  case proplists:get_value(allowed,Opts) of
    undefined -> do_prefix(Prefix,Fields,UseAs);
    Allowed ->
      FilterdFields = maps:with(Allowed,Fields),
      do_prefix(Prefix,FilterdFields,UseAs)
  end.

do_prefix(Prefix,Fields,true)->
  maps:fold(
    fun(Key,Attrs,Acc)->
        case proplists:lookup(as,Attrs) of
          {as,AsKey}-> [{as,{Prefix,Key},AsKey}|Acc];
          _ -> [{as,{Prefix,Key},Key}|Acc]
        end
    end,[],Fields);
do_prefix(Prefix,Fields,false) ->
  maps:fold(
    fun(Key,_Attrs,Acc)-> [{as,{Prefix,Key},Key}|Acc] end,
    [],Fields).



%%%===================================================================
%%% Internal functions
%%%===================================================================

read_key(Key,Attrs,Input)->
  case read_key(Key,Input) of
    undefined ->
      case proplists:lookup(as,Attrs) of
        {as,AsKey} -> read_key(AsKey,Input);
        _ -> undefined
      end;
    Value -> Value
end.

read_key(Key,Input)->
    case maps:is_key(Key,Input) of
        false ->
            KeyBin = ai_string:to_string(Key),
            case maps:is_key(KeyBin,Input) of
                false -> undefined;
                true -> {ok,maps:get(KeyBin,Input)}
            end;
        true -> {ok,maps:get(Key,Input)}
    end.
cast_value(Attrs,Value)->
  {type,Type} = proplists:lookup(type,Attrs),
  case ai_db_type:cast(Type,Value) of
    {ok,NewValue} -> NewValue;
    {error,Error} -> erlang:error(Error)
  end.
