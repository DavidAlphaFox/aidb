%%%-------------------------------------------------------------------
%%% @author David Gao <david@Davids-MacBook-Pro.local>
%%% @copyright (C) 2019, David Gao
%%% @doc
%%%
%%% @end
%%% Created : 22 Nov 2019 by David Gao <david@Davids-MacBook-Pro.local>
%%%-------------------------------------------------------------------
-module(ai_db_store).

-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, format_status/2]).

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

-define(SERVER, ?MODULE).

-record(state, {
                handler,
                handler_state
               }).

%%%===================================================================
%%% API
%%%===================================================================
persist(Name, Model) ->
  ai_pool:transaction(
    Name,
    fun(Worker)->
        gen_server:call(Worker,{persist,Model})
    end).
fetch(Name, ModelName,ID) ->
  ai_pool:transaction(
    Name,
    fun(Worker)->
        gen_server:call(Worker,{fetch,ModelName,ID})
    end).
delete_by(Name, ModelName, Conditions) ->
  ai_pool:transaction(
    Name,
    fun(Worker)->
        gen_server:call(Worker,{delete_by, ModelName, Conditions})
    end).
delete_all(Name, ModelName) ->
  ai_pool:transaction(
    Name,
    fun(Worker)->
        gen_server:call(Worker,{delete_all, ModelName})
    end).

find_all(Name, DocName) ->
  ai_pool:transaction(
    Name,
    fun(Worker)->
        gen_server:call(Worker,{find_all, DocName})
    end).
find_all(Name, DocName, SortFields, Limit, Offset) ->
  ai_pool:transaction(
    Name,
    fun(Worker)->
        gen_server:call(Worker,{find_all, DocName, SortFields, Limit, Offset})
    end).
find_by(Name, DocName, Conditions) ->
  ai_pool:transaction(
    Name,
    fun(Worker)->
        gen_server:call(Worker,{find_by, DocName, Conditions})
    end).
find_by(Name, DocName, Conditions, Limit, Offset) ->
  ai_pool:transaction(
    Name,
    fun(Worker)->
        gen_server:call(Worker,{find_by, DocName, Conditions, Limit, Offset})
    end).
find_by(Name, DocName, Conditions, SortFields, Limit, Offset) ->
  ai_pool:transaction(
    Name,
    fun(Worker)->
        gen_server:call(Worker, {find_by, DocName, Conditions, SortFields, Limit, Offset})
    end).
count(Name, DocName) ->
  ai_pool:transaction(
    Name,
    fun(Worker)->
        gen_server:call(Worker,{count, DocName})
    end).

count_by(Name, DocName, Conditions) ->
  ai_pool:transaction(
    Name,
    fun(Worker)->
        gen_server:call(Worker, {count_by, DocName, Conditions})
    end).
dirty(Name,Fun)->
  ai_pool:transaction(
    Name,
    fun(Worker)->
        gen_server:call(Worker, {dirty, Fun})
    end).
transaction(Name,Fun)->
  ai_pool:transaction(
    Name,
    fun(Worker)->
        gen_server:call(Worker, {transction, Fun})
    end).
%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%% @end
%%--------------------------------------------------------------------
-spec start_link(map()) -> {ok, Pid :: pid()} |
        {error, Error :: {already_started, pid()}} |
        {error, Error :: term()} |
        ignore.
start_link(Args) ->
  gen_server:start_link(?MODULE, Args, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> {ok, State :: term()} |
        {ok, State :: term(), Timeout :: timeout()} |
        {ok, State :: term(), hibernate} |
        {stop, Reason :: term()} |
        ignore.
init(Args) ->
  process_flag(trap_exit, true),
  Handler = map:get(store_handler,Args),
  {ok,HandlerState} = Handler:init(Args),
  {ok, #state{
          handler = Handler,
          handler_state = HandlerState
         }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), term()}, State :: term()) ->
        {reply, Reply :: term(), NewState :: term()} |
        {reply, Reply :: term(), NewState :: term(), Timeout :: timeout()} |
        {reply, Reply :: term(), NewState :: term(), hibernate} |
        {noreply, NewState :: term()} |
        {noreply, NewState :: term(), Timeout :: timeout()} |
        {noreply, NewState :: term(), hibernate} |
        {stop, Reason :: term(), Reply :: term(), NewState :: term()} |
        {stop, Reason :: term(), NewState :: term()}.
handle_call({persist, Model}, _From,
            #state{handler = Handler,
                   handler_state = HandlerState} = State) ->
  {Reply, NewState} = Handler:persist(Model,HandlerState),
  {reply, Reply, State#state{handler_state = NewState}};

handle_call({fetch, ModelName, Id}, _From,
            #state{handler = Handler,
                   handler_state = HandlerState} = State) ->
  { Reply, NewState} = Handler:fetch(ModelName, Id, HandlerState),
  {reply, Reply, State#state{handler_state=NewState}};

handle_call({delete_by, ModelName, Conditions}, _From,
            #state{handler = Handler,
                   handler_state = HandlerState} = State) ->
  { Reply, NewState} = Handler:delete_by(ModelName, Conditions, HandlerState),
  {reply, Reply, State#state{handler_state=NewState}};

handle_call({delete_all, ModelName}, _From,
            #state{handler = Handler,
                   handler_state = HandlerState} = State) ->
  {Reply, NewState} = Handler:delete_all(ModelName, HandlerState),
  {reply, Reply, State#state{handler_state=NewState}};

handle_call({find_all, ModelName}, _From,
            #state{handler = Handler,
                   handler_state = HandlerState} = State) ->
  {Reply, NewState} = Handler:find_all(ModelName, HandlerState),
  {reply, Reply, State#state{handler_state = NewState}};

handle_call({find_all, ModelName, SortFields, Limit, Offset}, _From,
            #state{handler = Handler,
                   handler_state = HandlerState} = State) ->
  { Reply, NewState} = Handler:find_all(
                         ModelName, SortFields, Limit, Offset, HandlerState),
  {reply, Reply, State#state{handler_state = NewState}};

handle_call({find_by, ModelName, Conditions}, _From,
            #state{handler = Handler,
                   handler_state = HandlerState} = State) ->
  { Reply, NewState} = Handler:find_by(ModelName, Conditions, HandlerState),
  {reply, Reply, State#state{handler_state=NewState}};

handle_call({find_by, ModelName, Conditions, Limit, Offset}, _From,
            #state{handler = Handler,
                   handler_state = HandlerState} = State) ->
  { Reply, NewState} = Handler:find_by(
                         ModelName, Conditions, Limit, Offset, HandlerState),
  {reply, Reply, State#state{handler_state=NewState}};

handle_call({find_by, ModelName, Conditions, SortFields, Limit, Offset}, _From,
            #state{handler = Handler,
                   handler_state = HandlerState} = State) ->
  { Reply, NewState} = Handler:find_by(
                         ModelName, Conditions, SortFields, Limit, Offset, HandlerState),
  {reply, Reply, State#state{handler_state=NewState}};

handle_call({count, ModelName}, _From,
            #state{handler = Handler,
                   handler_state = HandlerState} = State) ->
  { Reply, NewState} = Handler:count(ModelName, HandlerState),
  {reply, Reply, State#state{handler_state = NewState}};

handle_call({count_by, ModelName, Conditions}, _From,
            #state{handler = Handler,
                   handler_state = HandlerState} = State) ->
  { Reply, NewState} = Handler:count_by(ModelName, Conditions, HandlerState),
  {reply, Reply, State#state{handler_state = NewState}};
handle_call({dirty,Fun},_From,
            #state{handler = Handler,
                   handler_state = HandlerState} = State) ->
  {Reply, NewState} = Handler:dirty(Fun,HandlerState),
  {reply,Reply,State#state{handler_state = NewState}};
handle_call({transaction,Fun},_From,
            #state{handler = Handler,
                   handler_state = HandlerState} = State) ->
  {Reply, NewState} = Handler:transaction(Fun,HandlerState),
  {reply,Reply,State#state{handler_state = NewState}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: term()) ->
        {noreply, NewState :: term()} |
        {noreply, NewState :: term(), Timeout :: timeout()} |
        {noreply, NewState :: term(), hibernate} |
        {stop, Reason :: term(), NewState :: term()}.
handle_cast(_Request, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: term()) ->
        {noreply, NewState :: term()} |
        {noreply, NewState :: term(), Timeout :: timeout()} |
        {noreply, NewState :: term(), hibernate} |
        {stop, Reason :: normal | term(), NewState :: term()}.
handle_info(_Info, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: normal | shutdown | {shutdown, term()} | term(),
                State :: term()) -> any().
terminate(_Reason, _State) ->
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()},
                  State :: term(),
                  Extra :: term()) -> {ok, NewState :: term()} |
        {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called for changing the form and appearance
%% of gen_server status when it is returned from sys:get_status/1,2
%% or when it appears in termination error logs.
%% @end
%%--------------------------------------------------------------------
-spec format_status(Opt :: normal | terminate,
                    Status :: list()) -> Status :: term().
format_status(_Opt, Status) ->
  Status.

%%%===================================================================
%%% Internal functions
%%%===================================================================
