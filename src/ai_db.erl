-module(ai_db).

-export([start_pool/2]).

-define(SERVER,aidb_sup).

start_pool(postgres,Args)->
    PoolSpec = postgres(Args),
    supervisor:start_child(?SERVER,PoolSpec);
start_pool(redis,Args)->
    PoolSpec = redis(Args),
    supervisor:start_child(?SERVER,PoolSpec).

redis({Name,Opts})->
    PoolSize = maps:get(pool_size,Opts,5),
    MaxOverflow = maps:get(max_overflow,Opts,0),
    PoolBoyOpts =
        [{name,{local,Name}},{size,PoolSize},
         {max_overflow,MaxOverflow},{worker_module,ai_redis_worker}
        ],
    ai_pool:pool_spec(Name,PoolBoyOpts,Opts).
postgres({Name,Opts})->
    PoolSize = maps:get(pool_size,Opts,5),
    MaxOverflow = maps:get(max_overflow,Opts,0),
    PoolBoyOpts =
        [{name,{local,Name}},{size,PoolSize},
         {max_overflow,MaxOverflow},{worker_module,ai_postgres_worker}
        ],
    ai_pool:pool_spec(Name,PoolBoyOpts,Opts).
