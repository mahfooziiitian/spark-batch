# Complex join expression

        WITH person_modified AS (
         select
           id as personId,
           name,
           program_id as graduate_program,
           spark_status
        FROM person)
        SELECT * FROM
             person_modified
             INNER JOIN spark_status ON array_contains(spark_status, id)
