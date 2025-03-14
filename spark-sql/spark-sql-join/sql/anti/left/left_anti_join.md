# Left anti join

A left anti join is a type of join operation that returns `only the rows from the left` table that do not have corresponding matches in the right table.

It can be very useful for identifying records in one dataset that do not exist in another.

## Create table employees

  CREATE TABLE employee (
      id INT,
      name VARCHAR(50),
      age INT,
      department VARCHAR(50)
  );

    INSERT INTO employee (id, name, age, department)
    VALUES (1, 'John Doe', 30, 'IT');
    INSERT INTO employee (id, name, age, department)
    VALUES (2, 'Jane Smith', 25, 'HR'),
          (3, 'Michael Johnson', 35, 'Finance');
    
    INSERT INTO employee (id, name, age, department)
    VALUES (4, 'Mahfooz Doe', 30, 'HR');

  CREATE TABLE department (
      department_id INT ,
      department_name VARCHAR(50)
  );

  INSERT INTO department (department_id, department_name)
  VALUES (1, 'IT');

  INSERT INTO department (department_id, department_name)
  VALUES (2, 'HR'),
        (3, 'Finance');

  INSERT INTO department (department_id, department_name)
  VALUES (4, 'Admin');
  
## Joining

  select
    *
  from
    employee LEFT ANTI JOIN department on department_name = department;

  select
    *
  from
    department LEFT ANTI JOIN employee on department_name = department;
