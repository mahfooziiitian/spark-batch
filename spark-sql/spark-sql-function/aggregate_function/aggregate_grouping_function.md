# grouping

    SELECT 
        name, 
        grouping(name), 
        sum(age) 
    FROM 
        VALUES (2, 'Alice'), (5, 'Bob') people(age, name) 
    GROUP BY 
        cube(name);

    SELECT 
        name, 
        grouping_id(), 
        sum(age), 
        avg(height) 
    FROM 
        VALUES (2, 'Alice', 165), (5, 'Bob', 180) 
        people(age, name, height) 
    GROUP BY cube(name, height);
