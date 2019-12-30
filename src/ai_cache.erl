-module(ai_cache).
-export([start_pool/1]).
-export([cache/2,cache/3,
         hint/2,invalid/2]).

start_pool(Args) ->
    PoolSpec = store(Args),
    supervisor:start_child(ai_cache_pool_sup, PoolSpec).

cache(ModelName, UserModel) ->
    Model = ai_db_model:sleep(ModelName, UserModel),
    Store = ai_db_manager:store(ModelName,cache),
    case ai_cache_store:cache(Store, Model) of
        {ok, R} -> R;
        Error -> exit(Error)
    end.

cache(ModelName, UserModel,Expired) ->
    Model = ai_db_model:sleep(ModelName, UserModel),
    Store = ai_db_manager:store(ModelName,cache),
    case ai_cache_store:cache(Store, Model,Expired) of
        {ok, R} -> R;
        Error -> exit(Error)
    end.

hint(ModelName, CacheKey) ->
    Store = ai_db_manager:store(ModelName,cache),
    case ai_cache_store:hint(Store, ModelName, CacheKey) of
        {ok, Model} -> ai_db_model:wakeup(Model);
        {error,not_found} -> not_found;
        Error -> exit(Error)
    end.

invalid(ModelName,CacheKey)->
    Store = ai_db_manager:store(ModelName,cache),
    case ai_cache_store:invalid(Store, ModelName, CacheKey) of
        ok -> ok;
        Error -> exit(Error)
    end.
%%%=============================================================================
%%% Internal functions
%%%=============================================================================
store({Name, Opts}) ->
    PoolSize = maps:get(pool_size, Opts, 5),
    MaxOverflow = maps:get(max_overflow, Opts, 0),
    PoolBoyOpts = [{name, {local, Name}}, {size, PoolSize},
                   {max_overflow, MaxOverflow},
                   {worker_module, ai_cache_store}],
    ai_pool:pool_spec(Name, PoolBoyOpts, Opts).
