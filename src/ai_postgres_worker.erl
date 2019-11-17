-module(ai_postgres_worker).
-behaviour(gen_server).


-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, 
         terminate/2,code_change/3]).

-export([with_connection/2,with_transaction/2]).

-record(state, {conn, args,keepalive = 3600,timer = undefined,last_active = undefined}).


% public api
with_connection(Pid,Fun)->
    gen_server:call(Pid,{connection,Fun}).
with_transaction(Pid,Fun)->
    gen_server:call(Pid,{transaction,Fun}).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

% gen_server callbacks

init(Args) ->
    process_flag(trap_exit, true),
    KeepAlive = maps:get(keepalive,Args,3600),
    {ok, #state{conn = undefined, args=Args,keepalive = KeepAlive}}.



%% handle call
handle_call({connection,Fun},_From,State)->
    {Result,NewState} = run_connection(Fun,State),
    {reply,Result,NewState};
handle_call({transaction,Fun},_From,State)->
    {Result,NewState} = run_transction(Fun,State),
    {reply,Result,NewState}.



%% handle_cast

handle_cast(_Msg, State) ->
    {noreply, State}.


%% handle_info

handle_info({'EXIT',Conn,_}, #state{conn = Conn,timer = Timer}=State) ->
    ai_timer:cancel(Timer),
    {noreply, State#state{conn=undefined,last_active = undefined,timer = undefined}};
handle_info({keepalive,KeepAlive},#state{conn = Conn,timer = Timer,last_active = Active} = State)->
    Now = os:system_time(second),
    try 
        if Now - Active >= KeepAlive ->
                {ok,_} = epgsql:squery(Conn,"SELECT 1 "),
                State#state{timer = ai_timer:restart(Timer),last_active = Now};
           true ->
                State#state{timer = ai_timer:restart(Timer)}
        end        
    catch
        _:_ ->
            epgsql:close(Conn),
            {noreply,State#state{conn = undefined,last_active = undefined,timer = undefined}}
    end;

handle_info(_Info, State) ->
    {noreply, State}.


%% terminate

terminate(_Reason, #state{conn=undefined}) ->
    ok;

terminate(_Reason, #state{conn=Conn}) ->
    epgsql:close(Conn),
    ok.


%% code_change

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


% private functions


connect(#state{conn = Conn}=State) when is_pid(Conn) ->
    case is_process_alive(Conn) of
        true  -> {ok, Conn,State};
        false -> connect(State#state{conn = undefined})
    end;


connect(#state{conn = undefined, args = Args,keepalive = KeepAlive} = State) ->
    case epgsql:connect(Args) of
        {ok, Conn} -> 
            Timer = ai_timer:start(timer:seconds(KeepAlive),{keepalive,KeepAlive},ai_timer:new()),
            {ok, Conn,State#state{conn = Conn,timer = Timer,last_active = os:system_time(second)}};
        Error -> Error
    end.


run_transction(Fun,State)->
    case connect(State) of 
        {ok,Conn,NewState}-> 
            try
                R = epgsql:with_transaction(Conn,Fun,[{ensure_committed,true},{reraise,true}]),
                {R,NewState#state{last_active = os:system_time(second)}}
            catch
                _Reson:Error->
                    Timer = NewState#state.timer,
                    ai_timer:cancel(Timer),
                    epgsql:close(Conn),
                    {Error,NewState#state{conn = undefined,last_active = undefined,timer = undefined}}
            end;    
        Error-> {Error,State}
    end.

run_connection(Fun,State)->
    case connect(State) of 
        {ok,Conn,NewState}-> 
            try
                R =  Fun(Conn),
                {R,NewState#state{last_active = os:system_time(second)}}
            catch
                _Reson:Error->
                    Timer = NewState#state.timer,
                    ai_timer:cancel(Timer),
                    epgsql:close(Conn),
                    {Error,NewState#state{conn = undefined,last_active = undefined,timer = undefined}}
            end;
        Error -> { Error,State}
    end.
