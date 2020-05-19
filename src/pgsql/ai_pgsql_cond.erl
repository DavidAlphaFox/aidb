-module(ai_pgsql_cond).
-export([build/2]).
-record(state,{slot = 1, buffer = [],values = []}).
build(Slot,Exprs)->
  if
    (Exprs == []) or (Exprs == undefined)->
      {undefined,[],Slot};
    true ->
      State = transform(Exprs,#state{slot = Slot}),
      Cond =
        case State#state.buffer of
          [Clause] -> Clause;
          Clauses -> ai_string:join(
                       lists:reverse(Clauses),<<" AND ">>)
        end,
      {Cond,lists:reverse(State#state.values),State#state.slot}
  end.

transform(Exprs,State)
  when erlang:is_list(Exprs) ->
  lists:foldl(fun transform/2,State,Exprs);

transform({'and', Exprs},#state{buffer = OldBuffer} = State) ->
  NewState = transform(Exprs,State#state{buffer = []}),
  Clauses = ai_string:join(
              lists:reverse(NewState#state.buffer),<<" AND ">>),
  Clauses0 = <<" ( ",Clauses/binary," ) ">>,
  State#state{slot = NewState#state.slot,
              buffer = [Clauses0| OldBuffer],
              values = NewState#state.values};
transform({'or',Exprs},#state{buffer = OldBuffer} = State) ->
  NewState = transform(Exprs,State#state{buffer = []}),
  Clauses = ai_string:join(
              lists:reverse(NewState#state.buffer),<<" OR ">>),
  Clauses0 = <<" ( ",Clauses/binary," ) ">>,
  State#state{slot = NewState#state.slot,
              buffer = [Clauses0| OldBuffer],
              values = NewState#state.values};
transform({'not',{OP,Field,Value}},
          #state{slot = Slot,
                 buffer = OldBuffer,
                 values = OldValues} = State)
  when (OP == 'in')
       or (OP == 'like')
       or (OP == 'is')
       or (OP == 'between')
       or (OP == 'exist')->
  Operator =
    case OP of
      'is' -> <<" IS NOT ">>;
      _ ->
        OperatorBinary = operator(OP),
        <<" NOT",OperatorBinary/binary>>
    end,
  FieldBinary =  ai_pgsql_escape:field(Field),
  {ValueBinary,
   Values,
   NextSlot
  } = value(OP,Value,Slot),
  Clauses = <<FieldBinary/binary,
              Operator/binary,
              ValueBinary/binary>>,
  State#state{slot = NextSlot,
              buffer = [Clauses|OldBuffer],
              values = Values ++ OldValues
             };
transform({'not', Expr},#state{buffer = OldBuffer} = State) ->
  NewState = transform(Expr,State#state{buffer = []}),
  Clauses =
    case NewState#state.buffer of
      [One] -> One;
      Many ->
        Many0 = ai_string:join(
                  lists:reverse(Many),<<" AND ">>),
        <<" ( ",Many0/binary," ) ">>
    end,
  Clauses0 = <<" NOT ",Clauses/binary,$\s>>,
  State#state{slot = NewState#state.slot,
              buffer = [Clauses0|OldBuffer],
              values = NewState#state.values
             };
transform({Field,Value},State)-> transform({'==',Field,Value},State);
transform({OP,Field,Value},
          #state{slot = Slot,
                 buffer = OldBuffer,
                 values = OldValues} = State) ->
  FieldBinary = ai_pgsql_escape:field(Field),
  {ValueBinary,
   Values,
   NextSlot
  } = value(OP,Value,Slot),
  Operator = operator(OP),
  Clauses = <<FieldBinary/binary,
              Operator/binary,
              ValueBinary/binary>>,
  State#state{slot = NextSlot,
              buffer = [Clauses | OldBuffer],
              values = Values ++ OldValues};
transform(Fun,#state{slot = Slot,
                    buffer = OldBuffer,
                    values = OldValues} = State) ->
  {Clauses,
   Values,
   NextSlot
  } = erlang:apply(Fun, [Slot]),
  State#state{slot = NextSlot,
              buffer = [Clauses | OldBuffer],
              values = Values ++ OldValues}.

operator('in') -> <<" IN ">>;
operator('like') -> <<" LIKE ">>;
operator('is') -> <<" IS ">>;
operator('between') -> <<" BETWEEN ">>;
operator('exist') -> <<" EXIST ">>;
operator('=<') -> <<" <= ">>;
operator('/=') -> <<" <> ">>;
operator('==') -> <<" = ">>;
operator(OP) ->
  OPBin = ai_string:to_string(OP),
  <<$\s,OPBin/binary,$\s>>.
value(_,Value,Slot)
  when (Value == null) or (Value == undefined)->
  {<<" NULL ">>,[],Slot};
value(_,ValueFun,Slot)
  when erlang:is_function(ValueFun,1) ->
  erlang:apply(ValueFun, [Slot]);
value('in',ValueList,Slot)
  when erlang:is_list(ValueList)->
  InValues = lists:reverse(ValueList),
  Length = erlang:length(InValues),
  Holders = lists:seq(Slot,Slot + Length -1),
  Slots =
    lists:map(
      fun(H)-> ai_pgsql_escape:slot(H) end,
      Holders),
  Slots0 = ai_string:join(Slots,<<",">>),
  {<<" ( ",Slots0/binary," )">>,
   InValues,Slot + Length};
value(_,{field,Field},Slot)->
  FieldBinary = ai_pgsql_escape:field(Field),
  {FieldBinary,[],Slot};
value(_,Value,Slot) ->
  Slot0 = ai_pgsql_escape:slot(Slot),
  {Slot0,[Value],Slot + 1}.
