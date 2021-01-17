-module(trans_helper).
-export([build_atom/1,
         build_integer/1,
         build_float/1,
         build_string/1,
         build_list/1,
         build_tuple/1,
         build_boolean/1,
         build_binary/1
        ]).

-type ast() :: tuple() | [ast()].

-spec build_atom(atom()) -> ast().
build_atom(A) when is_atom(A) ->
  {atom, 1, A}.

%% @doc
%% ASTify an integer
%% @end
-spec build_integer(integer()) -> ast().
build_integer(I) when is_integer(I) ->
  {integer, 1, I}.

%% @doc
%% ASTify a float
%% @end
-spec build_float(float()) -> ast().
build_float(F) when is_float(F) ->
  {float, 1, F}.

%% @doc
%% ASTify a string
%% @end
-spec build_string(string()) -> ast().
build_string(S) when is_list(S) ->
  build_list(S).

%%% @doc
%%% ASTify a list
%%% @end
-spec build_list(list()) -> ast().
build_list(L) when is_list(L) ->
  IsString = is_string(L),
  if
    IsString =:= true -> {string, 1, L};
    true -> build_list_(L)
  end.
build_list_([]) -> {nil,1};
build_list_(L) ->
  [T|H] = lists:reverse(L),
  E = {cons, 1, build_value(T), {nil, 1}},
  do_build_list_(H, E).
do_build_list_([], E) -> E;
do_build_list_([T|H], R) ->
  E = {cons, 1, build_value(T), R},
  do_build_list_(H, E).

%% @doc
%% ASTify a tuple
%% @end
-spec build_tuple(tuple()) -> ast().
build_tuple(T) when is_tuple(T) ->
  TupleContent = lists:map(fun build_value/1, tuple_to_list(T)),
  {tuple, 1, TupleContent}.

%% @doc
%% ASTify a bin
%% @end
build_binary(S) when is_bitstring(S) ->
  LS = erlang:binary_to_list(S),
  IsString = is_string(LS),
  if
    IsString =:= true ->
      {bin, 1, [{bin_element, 1, {string, 1, LS}, default, default}]};
    true ->
      BinElements =
        lists:foldl(fun(E, Acc) ->
            IsString1 = is_string(E),
                        Acc ++ if
                                 IsString1 =:= true ->
                                   [{bin_element, 1, {string, 1, E}, default, default}];
                                 true ->
                                   [{bin_element, 1, {integer, 1, E}, default, default}]
                               end
        end, [], LS),
      {bin, 1, BinElements}
  end.

%% @doc
%% ASTify a boolean
%% @end
build_boolean(B) when is_boolean(B) ->
  build_atom(B).

%% @doc
%%% ASTify the given value
%% @end
build_value(X) when is_atom(X) -> build_atom(X);
build_value(X) when is_integer(X) -> build_integer(X);
build_value(X) when is_float(X) -> build_float(X);
build_value(X) when is_bitstring(X) -> build_binary(X);
build_value(X) when is_boolean(X) -> build_boolean(X);
build_value(X) when is_list(X) -> build_list(X);
build_value(X) when is_tuple(X) ->
  case is_ast(X) of
    true -> X;
    false -> build_tuple(X)
  end.

is_string(S) ->
  io_lib:printable_list(S) orelse io_lib:printable_unicode_list(S).

-spec is_ast(atom(), ast() | [ast()]) -> true | false.
is_ast(Type, AST) when is_tuple(AST) ->
  case Type of
    boolean -> element(1, AST) =:= atom andalso (element(3, AST) =:= true orelse element(3, AST) =:= false);
    any -> is_atom(element(1, AST)) and is_integer(element(2, AST));
    T ->
      First = element(1, AST),
      if
        First =:= T -> true;
        First =:= attribute -> element(3, AST) =:= Type;
        true -> false
      end
  end;
is_ast(Type, AST) when is_list(AST) ->
  lists:all(fun(E) ->
        is_ast(Type, E)
    end, AST);
is_ast(_, _) -> false.
-spec is_ast(ast() | [ast()]) -> true | false.
is_ast(AST) -> is_ast(any, AST).
