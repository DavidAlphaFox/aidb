-module(ai_postgres_worker).


-export([init/1]).

-export([
         dirty/2,
         transaction/2
        ]).

-include_lib("epgsql/include/epgsql.hrl").
-record(state, {conn, args}).

init(Args) -> {ok,#state{conn = undefined, args=Args}}.

dirty(Fun,State)->
    case connect(State) of
        {ok,Conn,NewState}->
            try
                R =  Fun(Conn),
                {R,NewState}
            catch
                _Type:Reason->
                    catch epgsql:close(Conn),
                    {{error,Reason},NewState#state{conn = undefined}}
            end;
        Error -> Error
    end.

transaction(Fun,State) ->
    case connect(State) of
        {ok,Conn,NewState}->
            try
                R = epgsql:with_transaction(Conn,Fun,[{ensure_committed,false},{reraise,true}]),
                {R,NewState}
            catch
                _Type:Reason->
                    catch epgsql:close(Conn),
                    {{error,Reason},NewState#state{conn = undefined}}
            end;
        Error-> Error
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================


alive(Conn) ->
    try
        {ok,_Col,_Row} = epgsql:squery(Conn,"SELECT 1 "),
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
    case epgsql:connect(Args) of
        {ok, Conn} -> {ok, Conn,State#state{conn = Conn}};
        Error -> {Error,State}
    end.

