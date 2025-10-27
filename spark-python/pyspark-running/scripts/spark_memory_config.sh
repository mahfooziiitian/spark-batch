#!/bin/bash
# spark_memory_config.sh - Enhanced Spark Memory Configuration Calculator

set -euo pipefail

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
print_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
print_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }

# Get system resources
TOTAL_MEMORY_KB=$(grep MemTotal /proc/meminfo | awk '{print $2}')
TOTAL_MEMORY_GB=$((TOTAL_MEMORY_KB / 1024 / 1024))
AVAILABLE_MEMORY_KB=$(grep MemAvailable /proc/meminfo | awk '{print $2}')
AVAILABLE_MEMORY_GB=$((AVAILABLE_MEMORY_KB / 1024 / 1024))
CORES=$(nproc)

# Memory allocation percentages
DRIVER_MEMORY_PERCENT=${DRIVER_MEMORY_PERCENT:-15}
EXECUTOR_MEMORY_PERCENT=${EXECUTOR_MEMORY_PERCENT:-60}
SYSTEM_RESERVED_PERCENT=${SYSTEM_RESERVED_PERCENT:-25}

# Calculate memory allocations
DRIVER_MEMORY=$((TOTAL_MEMORY_GB * DRIVER_MEMORY_PERCENT / 100))
EXECUTOR_MEMORY=$((TOTAL_MEMORY_GB * EXECUTOR_MEMORY_PERCENT / 100))
SYSTEM_RESERVED=$((TOTAL_MEMORY_GB * SYSTEM_RESERVED_PERCENT / 100))

# Minimum memory constraints
[ $DRIVER_MEMORY -lt 1 ] && DRIVER_MEMORY=1
[ $EXECUTOR_MEMORY -lt 1 ] && EXECUTOR_MEMORY=1

# Calculate executor instances and cores
EXECUTOR_CORES=${EXECUTOR_CORES:-2}
EXECUTOR_INSTANCES=$(((CORES - 1) / EXECUTOR_CORES))
[ $EXECUTOR_INSTANCES -lt 1 ] && EXECUTOR_INSTANCES=1

# Overhead calculations
EXECUTOR_MEMORY_OVERHEAD=$((EXECUTOR_MEMORY * 10 / 100))
[ $EXECUTOR_MEMORY_OVERHEAD -lt 384 ] && EXECUTOR_MEMORY_OVERHEAD=384

print_info "Spark Memory Configuration Calculator"
echo "================================================="

echo -e "\n${BLUE}System Resources:${NC}"
echo "  Total Memory: ${TOTAL_MEMORY_GB}GB"
echo "  Available Memory: ${AVAILABLE_MEMORY_GB}GB"
echo "  CPU Cores: $CORES"

echo -e "\n${BLUE}Memory Allocation Strategy:${NC}"
echo "  Driver Memory: ${DRIVER_MEMORY_PERCENT}% = ${DRIVER_MEMORY}GB"
echo "  Executor Memory: ${EXECUTOR_MEMORY_PERCENT}% = ${EXECUTOR_MEMORY}GB"
echo "  System Reserved: ${SYSTEM_RESERVED_PERCENT}% = ${SYSTEM_RESERVED}GB"

echo -e "\n${BLUE}Recommended Spark Configuration:${NC}"
echo "  --master local[$CORES]"
echo "  --driver-memory ${DRIVER_MEMORY}g"
echo "  --executor-memory ${EXECUTOR_MEMORY}g"
echo "  --executor-cores $EXECUTOR_CORES"
echo "  --num-executors $EXECUTOR_INSTANCES"
echo "  --executor-memory-overhead ${EXECUTOR_MEMORY_OVERHEAD}m"

# Performance warnings
echo -e "\n${YELLOW}Performance Recommendations:${NC}"
if [ $TOTAL_MEMORY_GB -lt 4 ]; then
    print_warn "Low memory system detected. Consider reducing parallelism."
fi

if [ $CORES -gt 8 ] && [ $EXECUTOR_CORES -eq 2 ]; then
    print_warn "High core count detected. Consider increasing executor cores to 4-5."
fi

# Generate spark-submit command
echo -e "\n${GREEN}Sample spark-submit command:${NC}"
cat << EOF
spark-submit \\
  --master local[$CORES] \\
  --driver-memory ${DRIVER_MEMORY}g \\
  --executor-memory ${EXECUTOR_MEMORY}g \\
  --executor-cores $EXECUTOR_CORES \\
  --num-executors $EXECUTOR_INSTANCES \\
  --executor-memory-overhead ${EXECUTOR_MEMORY_OVERHEAD}m \\
  your_application.py
EOF

# Export environment variables
export SPARK_DRIVER_MEMORY="${DRIVER_MEMORY}g"
export SPARK_EXECUTOR_MEMORY="${EXECUTOR_MEMORY}g"
export SPARK_EXECUTOR_CORES="$EXECUTOR_CORES"

print_success "Configuration complete. Environment variables exported."
