-record(ai_db_query,
        {
         action  :: atom(),
         table :: atom(),
         fields :: list(),
         where :: list(),
         extra_where :: list(),
         join :: list(),
         group_by :: list(),
         order_by :: list(),
         having :: list(),
         limit :: integer(),
         offset :: integer(),
         returning :: list()
        }).
-record(ai_db_query_context,
        {
         query :: term(),
         options :: list(),
         sql :: binary(),
         bindings :: list(),
         slot :: integer()
        }).
