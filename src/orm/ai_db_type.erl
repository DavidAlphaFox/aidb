-module(ai_db_type).
-export([is_datetime/1]).

is_datetime({_, _, _} = Date) ->
    calendar:valid_date(Date);
is_datetime({{_, _, _} = Date, {H, M, S}}) ->
    calendar:valid_date(Date) and
    (H >= 0 andalso H =< 23) and
    (M >= 0 andalso M =< 59) and
    (S >= 0 andalso S =< 59);
is_datetime(_) -> false.
