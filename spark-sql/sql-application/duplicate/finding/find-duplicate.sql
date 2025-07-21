-- Candidate Primary Key
SELECT COUNT(DISTINCT column_name) = COUNT(*) AS is_candidate_pk
FROM your_table;

