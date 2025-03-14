# Range join examples

## Create tables

    CREATE TABLE students_rj (
        id INT,
        name STRING,
        score INT
    )

    CREATE TABLE grade_range (
        grade STRING,
        min_score INT,
        max_score INT
    );

## Load data into it

    INSERT INTO students_rj (id, name, score) VALUES
    (1, 'Alice', 55),
    (2, 'Bob', 75),
    (3, 'Charlie', 85),
    (4, 'Diana', 65),
    (5, 'Eva', 70),
    (6, 'Frank', 90);

INSERT INTO grade_range
(grade, min_score, max_score)
VALUES ('A', 85, 100),
('B', 70, 84),
('C', 50, 69),
('D', 35, 49),
('F', 0, 34);

## Point in interval range join

    SELECT
        s.id,
        s.name,
        s.score,
        g.grade
    FROM
        students_rj s
    JOIN
        grade_range g
    ON
        s.score BETWEEN g.min_score AND g.max_score
