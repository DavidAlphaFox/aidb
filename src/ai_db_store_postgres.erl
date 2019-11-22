-module(ai_db_store_postgres).
-export([init/1]).

-export([
         persist/2,
         fetch/3,
         delete_by/3,
         delete_all/2,
         find_all/2,
         find_all/5,
         find_by/3,
         find_by/5,
         find_by/6,
         count/2,
         count_by/3,
         dirty/2,
         transaction/2
        ]).

-record(state, {conn, args}).

init(Args) -> {ok,#state{conn = undefined, args=Args}}.

persist(Model,State)-> {ok,State}.
fetch(ModelName,ID,State)-> {ok,State}.
delete_by(ModelName,ID,State) -> {ok,State}.
delete_all(ModelName,State) -> {ok, State}.
find_by(ModelName,Conditions,State) -> {ok,State}.
find_by(ModelName,Conditions,Limit,Offset,State) -> {ok,State}.
find_by(ModelName,Conditions,Sort,Limit,Offset,State) -> {ok,State}.
find_all(ModelName,State) -> {ok,State}.
find_all(ModelName,Sort,Limit,Offset,State) -> {ok,State}.
count(ModelName,State) -> {ok,State}.
count_by(ModelName,Conditions,State) -> {ok,State}.

dirty(Fun,State)->
    case connect(State) of
        {ok,Conn,NewState}->
            try
                R =  Fun(Conn),
                {R,NewState}
            catch
                _Reson:Error->
                    catch epgsql:close(Conn),
                    {Error,NewState#state{conn = undefined}}
            end;
        Error -> {Error,State}
    end.

transaction(Fun,State) ->
    case connect(State) of
        {ok,Conn,NewState}->
            try
                R = epgsql:with_transaction(Conn,Fun,[{ensure_committed,false},{reraise,true}]),
                {R,NewState}
            catch
                _Reson:Error->
                    catch epgsql:close(Conn),
                    {Error,NewState#state{conn = undefined}}
            end;
        Error-> {Error,State}
    end.

alive(Conn) ->
    try
        {ok,_} = epgsql:squery(Conn,"SELECT 1 "),
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
        Error -> Error
    end.
