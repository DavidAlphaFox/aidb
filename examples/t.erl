-module(t).
-compile({parse_transform,ai_pgsql_orm}).
-table('tb_test').
-field({id,   [{type, integer},{autoincrement, true}]}).
-field({name, [{type, string},{len, 128}]}).
-field({is_paid, [{type,boolean}]}).
-field({is_refund,[{type,boolean}]}).
