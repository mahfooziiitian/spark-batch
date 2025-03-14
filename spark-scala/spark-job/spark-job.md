# Job

With each action, the Spark scheduler builds an execution graph and launches a Spark job.

## Default scheduler

By default, Spark schedules jobs on a `first in, first out` basis.

However, Spark does offer a `fair scheduler`, which assigns tasks to `concurrent jobs` in `round-robin` fashion,
i.e., parceling out a few tasks for each job until the jobs are all complete.

The `fair scheduler` ensures that jobs get a more even share of cluster resources.

The Spark  application then launches jobs in the order that their corresponding `actions` were called on the `SparkContext`.

## The Anatomy of a Spark Job

In the Spark lazy evaluation paradigm, a Spark application doesn't "do anything" until the driver program calls an action.

With each action, the Spark scheduler builds an execution graph and launches a Spark job.


## Stage

## Task

