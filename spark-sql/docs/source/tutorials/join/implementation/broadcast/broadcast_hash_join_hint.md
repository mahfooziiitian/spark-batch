# Broadcast hash join hint

  select /*+ broadcast*/ *  from src s
    inner join hive_records r on s.key=r.key
