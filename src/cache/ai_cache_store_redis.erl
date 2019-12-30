-module(ai_cache_store_redis).
-export([init/1]).
-export([run/2]).

-record(state, {conn, args}).

init(Args) -> {ok,#state{conn = undefined, args=Args}}.


run(Fun,State)->
    case connect(State) of
        {ok,Conn,NewState}->
            try
                R =  Fun(Conn),
                {R,NewState}
            catch
                _:Reason ->
                    catch  eredis:stop(Conn),
                    {{error,Reason},NewState#state{conn = undefined}}
            end;
        Error -> Error
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

alive(Conn) ->
    try
        {ok,<<"PONG">>} = eredis:q(Conn,[<<"PING">>]),
        true
    catch
        _:_-> false
    end.

connect(#state{conn = Conn} = State) when is_pid(Conn) ->
    case is_process_alive(Conn) and alive(Conn) of
        true  -> {ok, Conn,State};
        false -> connect(State#state{conn = undefined})
    end;

connect(#state{conn = undefined, args = Args} = State) ->
    Host = maps:get(host,Args,"127.0.0.1"),
    Port = maps:get(port,Args,6379),
    Database = maps:get(database,Args,0),
    Password = maps:get(password,Args,""),
    ReconnectSleep = maps:get(reconnect_sleep,Args,100),
    ConnectTimeout = maps:get(connect_timeout,Args,5000),
    case eredis:start_link(Host, Port, Database, Password, ReconnectSleep, ConnectTimeout) of
        {ok, Conn} ->
            {ok, Conn,State#state{conn = Conn}};
        {error, Error} -> {error, Error}
    end.
