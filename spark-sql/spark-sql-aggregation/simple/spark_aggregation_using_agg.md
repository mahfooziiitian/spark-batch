# Aggregation

    select 
      state,
      min(Population) as minTotal,
      max(Population) as maxTotal 
      avg(Population) as avgTotal 
    from 
      states 
    group by 
      state 
    order by 
      minTotal desc
