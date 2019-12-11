-module(ai_db).

-export([start_pool/2]).
-export([register_model/3]).
-export([fetch/2, persist/2]).
-export([find_one/2,find_all/1,find_all/4,
         find_by/2,find_by/4,find_by/5]).
-export([delete_all/1,delete/2,delete_by/2]).
-export([count/1,count_by/2]).

start_pool(redis, Args) ->
    PoolSpec = redis(Args),
    supervisor:start_child(ai_redis_pool_sup, PoolSpec);
start_pool(store, Args) ->
    PoolSpec = store(Args),
    supervisor:start_child(ai_store_pool_sup, PoolSpec).

register_model(ModelName,ModelStore,ModelAttrs)->
  ai_db_manager:register(ModelName,ModelStore,ModelAttrs).

persist(ModelName, UserModel) ->
    Model = ai_db_model:sleep(ModelName, UserModel),
    Store = ai_db_manager:store(ModelName),
    case ai_db_store:persist(Store, Model) of
      {ok, NewModel} -> ai_db_model:wakeup(NewModel);
      Error -> exit(Error)
    end.

fetch(ModelName, Id) ->
    Store = ai_db_manager:store(ModelName),
    case ai_db_store:fetch(Store, ModelName, Id) of
      {ok, Model} -> ai_db_model:wakeup(Model);
      Error -> Error
    end.
find_one(ModelName, Conditions) ->
  case find_by(ModelName, Conditions, 1, 0) of
    []          -> not_found;
    [First | _] -> First
  end.
find_all(ModelName) ->
  case ai_db_store:find_all(ai_db_manager:store(ModelName), ModelName) of
    {ok, Models} -> models_wakeup(Models);
    Error      -> exit(Error)
  end.
find_all(ModelName, SortFields, Limit, Offset) ->
  case ai_db_store:find_all(ai_db_manager:store(ModelName),ModelName,SortFields,Limit,Offset) of
    {ok,Models} -> models_wakeup(Models);
    Error -> exit(Error)
  end.
find_by(ModelName,Conditions)->
  Store = ai_db_manager:store(ModelName),
  case ai_db_store:find_by(Store, ModelName, Conditions) of
    {ok, Models} -> models_wakeup(Models);
    Error      -> exit(Error)
  end.
find_by(ModelName, Conditions, Limit, Offset) ->
  Store = ai_db_manager:store(ModelName),
  case ai_db_store:find_by(Store, ModelName, Conditions, Limit, Offset) of
    {ok, Models} -> models_wakeup(Models);
    Error      -> exit(Error)
  end.

find_by(ModelName, Conditions, SortFields, Limit, Offset) ->
  NormalizedSortFields = normalize_sort_fields(SortFields),
  Store = ai_db_manager:store(ModelName),
  case ai_db_store:find_by(Store, ModelName, Conditions, NormalizedSortFields, Limit, Offset) of
    {ok, Models} -> models_wakeup(Models);
    Error      -> exit(Error)
  end.

delete_all(ModelName) ->
  Store = ai_db_manager:store(ModelName),
  case ai_db_store:delete_all(Store, ModelName) of
    {ok, NumRows} -> NumRows;
    Error -> exit(Error)
  end.

delete(ModelName, Id) ->
  IdField = ai_db_schema:id_name(ModelName),
  case delete_by(ModelName, [{IdField, Id}]) of
    1 -> true;
    0 -> false
  end.

delete_by(ModelName, Conditions) ->
  Store = ai_db_manager:store(ModelName),
  case ai_db_store:delete_by(Store, ModelName, Conditions) of
    {ok, 0} -> 0;
    {ok, NumRows} -> NumRows;
    Error -> exit(Error)
  end.

count(ModelName) ->
  case ai_db_store:count(ai_db_manager:store(ModelName), ModelName) of
    {ok, Total} -> Total;
    Error       -> exit(Error)
  end.
count_by(ModelName, Conditions) ->
  case ai_db_store:count_by(ai_db_manager:store(ModelName), ModelName, Conditions) of
    {ok, Total} -> Total;
    Error       -> exit(Error)
  end.
%%%=============================================================================
%%% Internal functions
%%%=============================================================================

redis({Name, Opts}) ->
    PoolSize = maps:get(pool_size, Opts, 5),
    MaxOverflow = maps:get(max_overflow, Opts, 0),
    PoolBoyOpts = [{name, {local, Name}}, {size, PoolSize},
		   {max_overflow, MaxOverflow},
		   {worker_module, ai_redis_worker}],
    ai_pool:pool_spec(Name, PoolBoyOpts, Opts).

store({Name, Opts}) ->
    PoolSize = maps:get(pool_size, Opts, 5),
    MaxOverflow = maps:get(max_overflow, Opts, 0),
    PoolBoyOpts = [{name, {local, Name}}, {size, PoolSize},
		   {max_overflow, MaxOverflow},
		   {worker_module, ai_db_store}],
    ai_pool:pool_spec(Name, PoolBoyOpts, Opts).

models_wakeup(Models) ->
  lists:map(fun(Model) -> ai_db_model:wakeup(Model) end, Models).

normalize_sort_fields(FieldName) when is_atom(FieldName) ->
  [{FieldName, asc}];
normalize_sort_fields({Name, Order}) ->
  [{Name, Order}];
normalize_sort_fields(SortFields) when is_list(SortFields) ->
  lists:flatmap(fun normalize_sort_fields/1, SortFields).
