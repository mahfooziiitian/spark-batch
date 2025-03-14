/*

A stage is a collection of tasks.

Spark splits a job into a DAG of stages.

A stage may depend on another stage.

For example, a job may be split into two stages, stage 0 and stage 1, where stage 1 cannot begin until stage 0 is completed.

Spark groups tasks into stages using shuffle boundaries.

Tasks that do not require a shuffle are grouped into the same stage.

A task that requires its input data to be shuffled begins a new stage.

 */
package spark.driver.job.stages;

public class Stages {
}
