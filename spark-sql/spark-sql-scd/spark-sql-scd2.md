# Scd2

## Create the source table

    CREATE TABLE source_data (
        id INT,
        name STRING,
        address STRING,
        load_date DATE
    );

## Create the dimension table

    CREATE TABLE dimension_table (
        id INT,
        name STRING,
        address STRING,
        effective_start_date DATE,
        effective_end_date DATE,
        is_current BOOLEAN
    );

## -- Insert initial data into source table

    INSERT INTO source_data VALUES
    (1, 'Alice', '123 Main St', date '2023-01-01'),
    (2, 'Bob', '456 Elm St', date '2023-01-01');

## Insert initial data into dimension table

    INSERT INTO dimension_table VALUES
    (1, 'Alice', '123 Main St', date '2023-01-01', date '9999-12-31', TRUE),
    (2, 'Bob', '456 Elm St', date '2023-01-01', date '9999-12-31', TRUE);

## Update Process

### New incoming data

    INSERT INTO source_data VALUES
    (1, 'Alice', '789 Oak St', date '2023-02-01'),  -- Alice has moved
    (3, 'Charlie', '111 Pine St', date '2023-02-01');  -- New employee Charlie

### Perform the merge operation

    MERGE INTO dimension_table AS d
    USING (
        SELECT id, name, address, load_date
        FROM source_data
    ) AS s
    ON d.id = s.id AND d.is_current = TRUE

    WHEN MATCHED AND (d.name <> s.name OR d.address <> s.address) THEN
        UPDATE SET d.is_current = FALSE, d.effective_end_date = s.load_date

    WHEN NOT MATCHED THEN
        INSERT (id, name, address, effective_start_date, effective_end_date, is_current)
        VALUES (s.id, s.name, s.address, s.load_date, '9999-12-31', TRUE);
