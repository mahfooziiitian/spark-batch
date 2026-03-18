#!/usr/bin/env bash
# Docker entrypoint — routes to the right executor based on the first argument.
set -euo pipefail

CMD="${1:-help}"
shift || true

# ── wait-for-port helper (used by `wait` command) ────────────────────────────
wait_for_port() {
  local host="$1" port="$2" timeout="${3:-30}"
  local elapsed=0
  echo "Waiting for ${host}:${port} (timeout ${timeout}s)..."
  while ! (echo > /dev/tcp/"$host"/"$port") 2>/dev/null; do
    sleep 1
    elapsed=$((elapsed + 1))
    if [ "$elapsed" -ge "$timeout" ]; then
      echo "ERROR: ${host}:${port} not reachable after ${timeout}s" >&2
      return 1
    fi
  done
  echo "${host}:${port} is ready."
}

case "$CMD" in

  run | python | python3)
    exec python3 "$@"
    ;;

  submit)
    exec /opt/spark/bin/spark-submit "$@"
    ;;

  test)
    if [ $# -eq 0 ]; then
      exec python3 -m pytest tests/
    else
      exec python3 -m pytest "$@"
    fi
    ;;

  shell | pyspark)
    exec /opt/spark/bin/pyspark "$@"
    ;;

  # Wait for a host:port to become available, then run a follow-up command
  # Usage: wait <host> <port> [timeout] -- <command...>
  wait)
    local host="$1" port="$2"
    shift 2
    local timeout=30
    if [ "${1:-}" != "--" ] && [ $# -gt 0 ]; then
      timeout="$1"; shift
    fi
    # consume the "--" separator
    if [ "${1:-}" = "--" ]; then shift; fi
    wait_for_port "$host" "$port" "$timeout"
    exec "$@"
    ;;

  sleep)
    exec tail -f /dev/null
    ;;

  help | --help | -h)
    cat <<'EOF'
Usage: docker run <image> <command> [args...]

Commands:
  run <script.py>                      Run a Python script with python3
  submit [spark-opts] <script.py>      Run spark-submit
  test [pytest-opts]                    Run pytest (defaults to tests/)
  shell                                Interactive PySpark shell
  wait <host> <port> [timeout] -- CMD  Wait for a TCP port, then exec CMD
  sleep                                Keep container alive (for docker exec)
  <anything else>                      Executed directly (e.g. bash, python3)
EOF
    ;;

  *)
    exec "$CMD" "$@"
    ;;

esac
