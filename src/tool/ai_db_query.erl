-module(ai_db_query).
-compile({inline,[do_prefix/2]}).
-export([cols/1,prefix/2,prefix/3]).

-spec prefix(Prefix::binary()|atom(),
            Fields::map())->list().
prefix(Prefix,Fields)->
  prefix(Prefix,Fields,undefined).

prefix(Prefix,Fields,undefined)-> do_prefix(Prefix,Fields);
prefix(Prefix,Fields,Allowed)->
  FilterdFields = maps:with(Allowed,Fields),
  do_prefix(Prefix,FilterdFields).

do_prefix(Prefix,Fields)->
  maps:fold(
    fun(Key,Attrs,Acc)->
        case proplists:lookup(as,Attrs) of
          {as,AsKey}-> [{as,{Prefix,Key},AsKey}|Acc];
          _ -> [{as,{Prefix,Key},Key}|Acc]
        end
    end,[],Fields).

-spec cols(Fields::map()) -> list().
cols(Fields)->
  maps:fold(
    fun(Key,Attrs,Acc)->
      case proplists:lookup(as,Attrs) of
        {as,AsKey} ->[{as,Key,AsKey}|Acc];
        _ -> [Key|Acc]
      end
    end,[],Fields).
