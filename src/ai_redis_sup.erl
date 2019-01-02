-module(ai_redis_sup).
-behaviour(supervisor).


-export([start_link/1]).
-export([init/1]).

%% {pool_name,Opts}

start_link(Conf) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Conf).



init(Conf) ->
    SupFlags = #{strategy => one_for_one,
                 intensity => 1,
                 period => 5},
    PoolSpecs =
        lists:map(fun({Name,Opts})->
                          PoolSize = 
                              case proplists:get_value(pool_size,Opts) of 
                                  undefined -> 5;
                                  Size -> Size
                              end,
                          PoolBoyOpts =
                              [{name,{local,Name}},{size,PoolSize},
                               {max_overflow,0},{worker_module,ai_redis_worker}
                              ],
                          poolboy:child_spec(Name,PoolBoyOpts,Opts)
                  end,Conf),
    {ok, {SupFlags, PoolSpecs}}.



