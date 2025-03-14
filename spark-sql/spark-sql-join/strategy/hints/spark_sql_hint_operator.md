# spark sql hint operator

The join strategy hints, namely `BROADCAST`, `MERGE`, `SHUFFLE_HASH` and `SHUFFLE_REPLICATE_NL`, instruct Spark to use the hinted strategy on each specified relation when joining them with another relation.

## BROADCAST hint

For example, when the BROADCAST hint is used on table 't1', broadcast join (either broadcast hash join or broadcast nested loop join depending on whether there is any equi-join key) with 't1' as the build side will be prioritized by Spark even if the size of table 't1' suggested by the statistics is above the configuration `spark.sql.autoBroadcastJoinThreshold`.

  SELECT
    /*+ BROADCAST(r)*/ *
  FROM
    records r
  JOIN src s
  ON r.key = s.key

## ORDER of hints

When different join strategy hints are specified on both sides of a join, Spark prioritizes the BROADCAST hint over the MERGE hint over the SHUFFLE_HASH hint over the SHUFFLE_REPLICATE_NL hint.

When both sides are specified with the BROADCAST hint or the SHUFFLE_HASH hint, Spark will pick the build side based on the join type and the sizes of the relations.
