-module(ai_redis_worker).
-behaviour(gen_server).
-behaviour(poolboy_worker).


-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([with_connection/2]).

-record(state, {conn, host, port, 
                database, password, 
                reconnect_sleep, 
                connect_timeout
               }).


% public api
with_connection(Pid,Fun)->
    gen_server:call(Pid,{connection,Fun}).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

% gen_server callbacks

init(Args) ->
    process_flag(trap_exit, true),
    Host = maps:get(host,Args,"127.0.0.1"),
    Port = maps:get(port,Args,6379),
    Database = maps:get(database,Args,0),
    Password = maps:get(password,Args,""),
    ReconnectSleep = maps:get(reconnect_sleep,Args,100),
    ConnectTimeout = maps:get(connect_timeout,Args,5000),
    {ok, #state{conn = undefined,host = Host,port = Port,
                database = Database,password = Password,
                reconnect_sleep = ReconnectSleep,
                connect_timeout = ConnectTimeout }}.



%% handle call
handle_call({connection,Fun},_From,State)->
    {Result,NewState} = run_connection(Fun,State),
    {reply,Result,NewState};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.


%% handle_cast

handle_cast(_Msg, State) ->
    {noreply, State}.


%% handle_info

handle_info({'EXIT',Conn,_}, #state{conn = Conn}=State) ->
    {noreply, State#state{conn=undefined}};

handle_info(_Info, State) ->
    {noreply, State}.


%% terminate

terminate(_Reason, #state{conn=undefined}) ->
    ok;

terminate(_Reason, #state{conn=Conn}) ->
    eredis:stop(Conn),
    ok.


%% code_change

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


    
connect(#state{conn = Conn}=State) when is_pid(Conn) ->
    case is_process_alive(Conn) of
        true  -> {ok, Conn,State};
        false -> connect(State#state{conn = undefined})
    end;


connect(#state{
           host = Host,port = Port,
           database = Database,password = Password,
           reconnect_sleep = ReconnectSleep, connect_timeout = ConnectTimeout
          } = State) ->
    case eredis:start_link(Host, Port, Database, Password, ReconnectSleep, ConnectTimeout) of
        {ok, Conn} -> {ok, Conn,State#state{conn = Conn}};
        {error, Error} -> {error, Error}
    end.


run_connection(Fun,State)->
    case connect(State) of 
        {ok,Conn,NewState}-> 
            try
                R = Fun(Conn),
                {R,NewState}
             catch
                 _Reson:Error -> 
                     eredis:stop(Conn),
                     {{error,Error},NewState#state{conn = undefined}}
             end;
        {error,Error}-> {{error, Error},State}
    end.

