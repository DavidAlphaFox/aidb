%%%-------------------------------------------------------------------
%%% @author David Gao <david@Davids-MacBook-Pro.local>
%%% @copyright (C) 2019, David Gao
%%% @doc
%%%
%%% @end
%%% Created : 22 Feb 2019 by David Gao <david@Davids-MacBook-Pro.local>
%%%-------------------------------------------------------------------
-module(aidb_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%%===================================================================
%%% Application callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called whenever an application is started using
%% application:start/[1,2], and should start the processes of the
%% application. If the application is structured according to the OTP
%% design principles as a supervision tree, this means starting the
%% top supervisor of the tree.
%% @end
%%--------------------------------------------------------------------
-spec start(StartType :: normal |
	{takeover, Node :: node()} |
	{failover, Node :: node()},
	StartArgs :: term()) ->
	{ok, Pid :: pid()} |
	{ok, Pid :: pid(), State :: term()} |
	{error, Reason :: term()}.
start(_StartType, _StartArgs) ->
		application:start(crypto),
		application:start(asn1),
		application:start(public_key),
		application:start(ssl),
    application:start(epgsql),
    application:start(eredis),
    application:start(ailib),
    aidb_sup:start_link().


-spec stop(State :: term()) -> any().
stop(_State) -> ok.
