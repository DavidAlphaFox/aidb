-module(ai_db).

-export([start_pool/1]).
-export([
         dirty/2,
         transaction/2
        ]).

start_pool(Args) ->
  PoolSpec = store(Args),
  supervisor:start_child(ai_db_pool_sup, PoolSpec).

dirty(Name,Fun)->
  ai_pool:transaction(
    Name,
    fun(Worker)->
        gen_server:call(Worker, {dirty, Fun})
    end).
transaction(Name,Fun)->
  ai_pool:transaction(
    Name,
    fun(Worker)->
        gen_server:call(Worker, {transaction, Fun})
    end).


%%%=============================================================================
%%% Internal functions
%%%=============================================================================


store({Name, Opts}) ->
  PoolSize = maps:get(pool_size, Opts, 5),
  MaxOverflow = maps:get(max_overflow, Opts, 0),
  PoolBoyOpts = [{name, {local, Name}}, {size, PoolSize},
                 {max_overflow, MaxOverflow},
                 {worker_module, ai_db_worker}],
  ai_pool:pool_spec(Name, PoolBoyOpts, Opts).

