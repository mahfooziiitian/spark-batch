# Broadcast hash join

  select
    /*+ MAPJOIN(departments)*/ *
  from
    employees JOIN departments on dept_no == id

  select
    /*+ BROADCAST(departments)*/ *
  from employees
    JOIN departments on dept_no == id
