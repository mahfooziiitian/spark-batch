COPY persons(first_name, last_name, dob, email)
FROM '/d/data/database/postgres/data/persons.csv'
DELIMITER ','
CSV HEADER;