from pyspark.sql import Row

employee_data = [
    Row(id=1, employee_name="Homer Simpson", department_id=4),
    Row(id=2, employee_name="Ned Flanders", department_id=1),
    Row(id=3, employee_name="Barney Gumble", department_id=5),
    Row(id=4, employee_name="Clancy Wiggum", department_id=3),
    Row(id=5, employee_name="Moe Syzslak", department_id=None),
]

# Define the schema
employee_schema = ["id", "employee_name", "department_id"]

department_data = [
    Row(department_id=1, department_name="Sales"),
    Row(department_id=2, department_name="Engineering"),
    Row(department_id=3, department_name="Human Resources"),
    Row(department_id=4, department_name="Customer Service"),
    Row(department_id=5, department_name="Research And Development"),
]

department_schema = ["department_id", "department_name"]

salary_data = [
    Row(employee_id=1, current_salary=60000),
    Row(employee_id=2, current_salary=75000),
    Row(employee_id=3, current_salary=50000),
]

# Define the schema
salary_schema = ["employee_id", "current_salary"]
