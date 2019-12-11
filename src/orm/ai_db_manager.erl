%%%-------------------------------------------------------------------
%%% @author David Gao <david@Davids-MacBook-Pro.local>
%%% @copyright (C) 2019, David Gao
%%% @doc
%%%
%%% @end
%%% Created : 22 Nov 2019 by David Gao <david@Davids-MacBook-Pro.local>
%%%-------------------------------------------------------------------
-module(ai_db_manager).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, format_status/2]).

-export([models/0,model/1,store/1,store/2]).
-export([attrs/1,attr_value/2,attr_value/3]).
-export([register/3]).

-define(SERVER, ?MODULE).

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================
register(ModelName,ModelStore,ModelAttrs)->
    gen_server:call(?MODULE,{register,ModelName,ModelStore,ModelAttrs}).

models() ->
    MS = ets:fun2ms(fun({X, Y, Z}) -> {X, Y, Z} end),
    ets:select(?MODULE, MS).

model(ModelName) ->
    case ets:lookup(?MODULE,ModelName) of
      [Model] -> Model;
      []    -> undefined
    end.

store(ModelName) ->
    case lookup_element(ModelName, 2) of
        M when erlang:is_map(M)-> maps:get(primary,M);
        Store -> Store
    end.
store(ModelName,Type)->
    Store = lookup_element(ModelName, 2),
    maps:get(Type,Store).

attrs(ModelName) ->
    lookup_element(ModelName, 3).

attr_value(ModelName, Prop) ->
    attr_value(ModelName, Prop, undefined).

attr_value(ModelName, Prop, Default) ->
  maps:get(Prop, attrs(ModelName), Default).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> {ok, Pid :: pid()} |
          {error, Error :: {already_started, pid()}} |
          {error, Error :: term()} |
          ignore.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

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
init([]) ->
    process_flag(trap_exit, true),
    case ets:info(?MODULE) of
        undefined -> ok;
        _ ->
            true = ets:delete(?MODULE)
    end,
    ?MODULE = ets:new(?MODULE, [named_table, public, {read_concurrency, true}]),
    {ok, #state{}}.

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
handle_call({register,ModelName,ModelStore,ModelAttrs},_From,State)->
    R = ets:insert(?MODULE, {ModelName,ModelStore,ModelAttrs}),
    {reply,R,State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

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
%% of gen_server status when it is returned from sys:status/1,2
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
lookup_element(Key, Pos) ->
    lookup_element(Key, Pos, undefined).

%% @private
lookup_element(Key, Pos, Default) ->
    try ets:lookup_element(?MODULE, Key, Pos)
    catch
        _:_ -> Default
    end.
