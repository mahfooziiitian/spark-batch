# Avg

    select 
      avg(count) as avg_fun,
      sum(count)/count(count) as avg
    from flight_summary;
