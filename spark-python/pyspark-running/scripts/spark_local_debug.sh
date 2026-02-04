#!/bin/bash
# spark_local_debug.sh - Enhanced Spark local debugging script

set -euo pipefail  # Exit on error, undefined vars, pipe failures

# Configuration
SPARK_HOME=${SPARK_HOME:-"/usr/local/spark"}
LOG_LEVEL=${LOG_LEVEL:-"INFO"}
LOG_DIR=${LOG_DIR:-"/tmp/spark-logs"}
DRIVER_MEMORY=${DRIVER_MEMORY:-"2g"}
EXECUTOR_MEMORY=${EXECUTOR_MEMORY:-"2g"}
CORES=${CORES:-"4"}
JOB_NAME=${JOB_NAME:-"DebuggingJob"}
SPARK_APP=${1:-"spark_debug_job.py"}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

cleanup() {
    log_info "Cleaning up old log files older than 7 days..."
    find "$LOG_DIR" -name "*.log" -mtime +7 -delete 2>/dev/null || true
}

check_prerequisites() {
    if [[ ! -d "$SPARK_HOME" ]]; then
        log_error "SPARK_HOME directory not found: $SPARK_HOME"
        exit 1
    fi
    
    if [[ ! -f "$SPARK_HOME/bin/spark-submit" ]]; then
        log_error "spark-submit not found in: $SPARK_HOME/bin/"
        exit 1
    fi
    
    if [[ ! -f "$SPARK_APP" ]]; then
        log_error "Spark application not found: $SPARK_APP"
        exit 1
    fi
}

show_usage() {
    cat << EOF
Usage: $0 [spark_app.py]

Environment Variables:
  SPARK_HOME      Path to Spark installation (default: /opt/spark)
  LOG_LEVEL       Spark log level (default: INFO)
  LOG_DIR         Directory for logs (default: /tmp/spark-logs)
  DRIVER_MEMORY   Driver memory (default: 2g)
  EXECUTOR_MEMORY Executor memory (default: 2g)
  CORES           Number of cores (default: 4)
  JOB_NAME        Spark job name (default: DebuggingJob)

Examples:
  $0                          # Run with spark_debug_job.py
  $0 my_app.py               # Run with specific app
  LOG_LEVEL=DEBUG $0         # Run with debug logging
  CORES=8 DRIVER_MEMORY=4g $0 # Run with more resources
EOF
}

# Main execution
main() {
    if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
        show_usage
        exit 0
    fi
    
    log_info "Starting Spark local debug session"
    log_info "Application: $SPARK_APP"
    log_info "Cores: $CORES, Driver Memory: $DRIVER_MEMORY, Executor Memory: $EXECUTOR_MEMORY"
    
    check_prerequisites
    cleanup
    
    # Create log directory
    mkdir -p "$LOG_DIR"
    
    # Generate unique session ID
    SESSION_ID=$(date +"%Y%m%d_%H%M%S")_$$
    
    log_info "Session ID: $SESSION_ID"
    log_info "Logs will be saved to: $LOG_DIR"
    
    # Enhanced Spark configuration
    "$SPARK_HOME/bin/spark-submit" \
        --master "local[$CORES]" \
        --driver-memory "$DRIVER_MEMORY" \
        --executor-memory "$EXECUTOR_MEMORY" \
        --conf "spark.app.name=$JOB_NAME-$SESSION_ID" \
        --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties -XX:+UseG1GC" \
        --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC" \
        --conf "spark.eventLog.enabled=true" \
        --conf "spark.eventLog.dir=$LOG_DIR" \
        --conf "spark.history.fs.logDirectory=$LOG_DIR" \
        --conf "spark.sql.adaptive.enabled=true" \
        --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
        --conf "spark.sql.adaptive.logLevel=$LOG_LEVEL" \
        --conf "spark.sql.warehouse.dir=/tmp/spark-warehouse" \
        --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
        --conf "spark.local.dir=/tmp/spark-temp" \
        --conf "spark.ui.retainedJobs=50" \
        --conf "spark.ui.retainedStages=50" \
        --conf "spark.sql.execution.arrow.pyspark.enabled=true" \
        "$SPARK_APP" \
        2>&1 | tee "$LOG_DIR/spark-session-$SESSION_ID.log"
    
    local exit_code=${PIPESTATUS[0]}
    
    if [[ $exit_code -eq 0 ]]; then
        log_info "Spark job completed successfully"
    else
        log_error "Spark job failed with exit code: $exit_code"
    fi
    
    log_info "Event logs saved to: $LOG_DIR"
    log_info "Session log: $LOG_DIR/spark-session-$SESSION_ID.log"
    log_info "Access Spark UI at: http://localhost:4040"
    log_info "Access Spark History Server: $SPARK_HOME/sbin/start-history-server.sh"
    
    return $exit_code
}

# Run main function
main "$@"