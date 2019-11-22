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
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).


-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

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
init([]) ->
    SupFlags = #{strategy => one_for_one,
                 intensity => 5,
                 period => 5},
    DBManager = #{ id => ai_db_manager,
                   start => {ai_db_manager,start_link,[]},
                   restart => transient,
                   shutdown => 5000,
                   type => worker,
                   modules => [ai_db_manager]
                 },
    RedisSup = #{id => ai_redis_pool_sup,
                 start => {ai_temp_sup,
                           start_link,
                           [#{name => {local,ai_redis_pool_sup},
                              strategy => one_for_one,
                              intensity => 5,period => 5}
                           ]},
                 restart => transient,
                 shutdown => 5000,
                 type => supervisor,
                 modules => [ai_temp_sup]},

    PostgresSup = #{id => ai_postgres_pool_sup,
                    start => {ai_temp_sup,
                              start_link,
                              [#{name => {local,ai_postgres_pool_sup},
                                 strategy => one_for_one,
                                 intensity => 5,period => 5}
                              ]},
                    restart => transient,
                    shutdown => 5000,
                    type => supervisor,
                    modules => [ai_temp_sup]},
    {ok, {SupFlags,[DBManager,PostgresSup,RedisSup]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
