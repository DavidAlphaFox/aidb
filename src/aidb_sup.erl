%%%-------------------------------------------------------------------
%%% @author David Gao <david@Davids-MacBook-Pro.local>
%%% @copyright (C) 2019, David Gao
%%% @doc
%%%
%%% @end
%%% Created : 22 Feb 2019 by David Gao <david@Davids-MacBook-Pro.local>
%%%-------------------------------------------------------------------
-module(aidb_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-export([start_redis/1,start_postgres/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================
start_postgres(Args)->
    PoolSpec = postgres(Args),
    supervisor:start_child(?SERVER,PoolSpec).

start_redis(Args)->
    PoolSpec = redis(Args),
    supervisor:start_child(?SERVER,PoolSpec).

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> {ok, Pid :: pid()} |
                      {error, {already_started, Pid :: pid()}} |
                      {error, {shutdown, term()}} |
                      {error, term()} |
                      ignore.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

start_link(Args)->
    supervisor:start_link({local, ?SERVER}, ?MODULE, Args).
%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart intensity, and child
%% specifications.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
                  {ok, {SupFlags :: supervisor:sup_flags(),
                        [ChildSpec :: supervisor:child_spec()]}} |
                  ignore.
init(Args) ->
    SupFlags = #{strategy => one_for_one,
                 intensity => 5,
                 period => 5},
    PoolSpecs = lists:foldl(fun
                                ({postgres,Conf},Acc)->
                                    [postgres(Conf)|Acc];
                                ({redis,Conf},Acc) ->
                                    [redis(Conf)|Acc];
                                (_,Acc) -> Acc
                            end,[],Args),
    {ok, {SupFlags, PoolSpecs}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
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
