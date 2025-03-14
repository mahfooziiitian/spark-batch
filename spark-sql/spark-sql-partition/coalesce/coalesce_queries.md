# coalesce

Coalesce hints allow Spark SQL users to control the number of output files just like coalesce, repartition and repartitionByRange in the Dataset API, they can be used for performance tuning and reducing the number of output files.

The "COALESCE" hint only has a partition number as a parameter.

  SELECT /*+ COALESCE(3)*/ * FROM t;
