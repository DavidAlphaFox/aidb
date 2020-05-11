-module(ai_redis_worker).
-export([init/1,handle_info/2,terminate/1]).
-export([
         execute/2,
         transaction/2
        ]).


-record(state,
        {conn, host, port, database, password, reconnect_sleep,
         connect_timeout, keepalive, timer, last_active}).


init(Args)->
    Host = maps:get(host, Args, "127.0.0.1"),
    Port = maps:get(port, Args, 6379),
    Database = maps:get(database, Args, 0),
    Password = maps:get(password, Args, ""),
    ReconnectSleep = maps:get(reconnect_sleep, Args, 100),
    ConnectTimeout = maps:get(connect_timeout, Args, 5000),
    KeepAlive = maps:get(keepalive, Args, 3600),
    {ok,
     #state{conn = undefined, host = Host, port = Port,
            database = Database, password = Password,
            reconnect_sleep = ReconnectSleep,
            connect_timeout = ConnectTimeout,
            keepalive = KeepAlive}}.

handle_info({'EXIT', Conn, _},
            #state{conn = Conn} = State) ->
  State#state{conn = undefined};
handle_info({keepalive, KeepAlive},
            #state{conn = Conn, timer = Timer,
                   last_active = Active} =
              State) ->
  Now = os:system_time(second),
  try
    if
      Now - Active >= KeepAlive ->
        {ok, <<"PONG">>} = eredis:q(Conn, [<<"PING">>]),
        State#state{timer = ai_timer:restart(Timer),
                    last_active = Now};
       true -> State#state{timer = ai_timer:restart(Timer)}
    end
  catch
    _:_ ->
      eredis:stop(Conn),
      State#state{conn = undefined, last_active = undefined,
                  timer = undefined}
  end.
terminate(#state{conn = undefined}) -> ok;
terminate(#state{conn = Conn}) ->
  eredis:stop(Conn),
  ok.

execute(Fun, State) ->
  case connect(State) of
    {ok, Conn, NewState} ->
      try
        R = Fun(Conn),
        {R,NewState#state{last_active = os:system_time(second)}}
      catch
        _:Error ->
          Timer = NewState#state.timer,
          ai_timer:cancel(Timer),
          catch eredis:stop(Conn),
          {{error, Error},
           NewState#state{conn = undefined,
                          last_active = undefined,
                          timer = undefined}}
      end;
    Error -> Error
  end.

transaction(_Fun,State)->
  {{error,method_not_support},State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
connect(#state{conn = Conn} = State)
  when is_pid(Conn) ->
  case is_process_alive(Conn) of
    true -> {ok, Conn, State};
    false -> connect(State#state{conn = undefined})
  end;
connect(#state{host = Host, port = Port,
               database = Database, password = Password,
               reconnect_sleep = ReconnectSleep,
               connect_timeout = ConnectTimeout,
               keepalive = KeepAlive} = State) ->
  case eredis:start_link(
         Host, Port, Database, Password,
         ReconnectSleep, ConnectTimeout) of
    {ok, Conn} ->
      Timer = ai_timer:start(timer:seconds(KeepAlive),
                             {keepalive, KeepAlive}, ai_timer:new()),
      {ok, Conn,
       State#state{conn = Conn, timer = Timer,
                   last_active = os:system_time(second)}};
    {error, Error} -> {{error, Error},State}
  end.
