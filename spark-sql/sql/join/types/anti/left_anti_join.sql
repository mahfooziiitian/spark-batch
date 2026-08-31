-- Databricks notebook source
--- Create table ozd.poc.employees
CREATE TABLE ozd.poc.employee (
    id INT,
    name VARCHAR(50),
    age INT,
    department VARCHAR(50)
); -- COMMAND ----------
-- Inserting data into ozd.poc.employee table
INSERT INTO ozd.poc.employee (id, name, age, department)
VALUES (1, 'John Doe', 30, 'IT');
INSERT INTO ozd.poc.employee (id, name, age, department)
VALUES (2, 'Jane Smith', 25, 'HR'),
    (3, 'Michael Johnson', 35, 'Finance');
INSERT INTO ozd.poc.employee (id, name, age, department)
VALUES (4, 'Mahfooz Doe', 30, 'HR');
-- COMMAND ----------
CREATE TABLE ozd.poc.department (
    department_id INT,
    department_name VARCHAR(50)
);
-- COMMAND ----------
INSERT INTO ozd.poc.department (department_id, department_name)
VALUES (1, 'IT');
INSERT INTO ozd.poc.department (department_id, department_name)
VALUES (2, 'HR'),
    (3, 'Finance');
INSERT INTO ozd.poc.department (department_id, department_name)
VALUES (4, 'Admin');
-- COMMAND ----------
-- Joining
select *
from ozd.poc.employee LEFT ANTI
    JOIN ozd.poc.department on department_name = department;
select *
from ozd.poc.department LEFT ANTI
    JOIN ozd.poc.employee on department_name = department;