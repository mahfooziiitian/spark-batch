#!/usr/bin/env bash
# Runner for the pyspark-ds-api example scenarios.
#
# Each scenario is either:
#   - "paired"     : a mock FastAPI server + an ETL/runner script that calls it
#   - "standalone" : a single script that talks to a live external API or
#                     needs no network at all
#
# Usage:
#   examples/run_examples.sh list                 # list all scenarios
#   examples/run_examples.sh run <name> [-- args] # run one scenario
#   examples/run_examples.sh all [--include-manual]
#
# Scenarios that require manual interaction or third-party credentials
# (browser-based OAuth2 flows, GitHub device code, etc.) are flagged MANUAL
# and are skipped by `all` unless --include-manual is passed.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DEFAULT_PORT_READY_TIMEOUT=20
DEFAULT_ETL_TIMEOUT=90
# Mock server output is noisy and, if left attached to this script's own
# stdout/stderr, has been observed to disrupt this script's own output once
# the server is killed (likely a fd-sharing quirk between the backgrounded
# uvicorn/JVM child and the parent shell). Redirecting each server's output
# to its own log file keeps it fully isolated from the parent's fds.
LOG_DIR="${REPO_ROOT}/examples/.run_logs"

# name|server_script|port|etl_script|manual|note
SCENARIOS="
auth-basic|authentication/basic/api_basic_source.py|8081|authentication/basic/api_basic_authentication.py|no|
auth-api-key-header|authentication/api_key/header/api_key_header_source.py|8081|authentication/api_key/header/api_key_header.py|no|
auth-api-key-query|-|-|authentication/api_key/query/api_key_query.py|no|hits the live openweathermap API
auth-jwt-bearer|-|-|authentication/jwt/api_bearer_token.py|no|hits a live token endpoint
auth-mtls|authentication/mtls/api_mtls_source.py|8443|authentication/mtls/api_mtls_etl.py|no|requires certs (auto-generated via generate_mtls_certs.sh)
auth-certificates|-|-|authentication/certificates/api_certificates_only.py|no|hits the live openweathermap API
auth-certificates-validate|-|-|authentication/certificates/validate_certificates.py|yes|needs client_cert.pem/ca_bundle.pem; pass --skip-request via '--'
auth-etl-extract|-|-|authentication/etl_extract.py|no|generic runner, defaults to the api_key_query scenario
auth-oauth2-client-credentials-form|authentication/oauth2/client_credential/form/api_oauth2_client_credentials_form_source.py|8081|authentication/oauth2/client_credential/form/api_oauth2_client_credentials_form_etl.py|no|
auth-oauth2-client-credentials-json|authentication/oauth2/client_credential/json/api_oauth2_client_credentials_json_source.py|8081|authentication/oauth2/client_credential/json/api_oauth2_client_credentials_json_etl.py|no|
auth-oauth2-client-credentials-form-basic|authentication/oauth2/client_credential/basic/api_oauth2_client_credentials_form_basic_source.py|8081|authentication/oauth2/client_credential/basic/api_oauth2_client_credentials_form_basic_etl.py|no|
auth-oauth2-client-credentials-basic-dbx|authentication/oauth2/client_credential/basic/api_oauth2_client_credentials_form_basic_source.py|8081|authentication/oauth2/client_credential/basic/api_oauth2_client_credentials_basic_dbx_etl.py|no|reuses the form-basic mock server
auth-oauth2-password-form|authentication/oauth2/password/form/api_oauth2_password_source.py|8081|authentication/oauth2/password/form/api_oauth2_password_etl.py|no|
auth-oauth2-password-json|authentication/oauth2/password/json/api_oauth2_password_json_source.py|8081|authentication/oauth2/password/json/api_oauth2_password_json_etl.py|no|
auth-oauth2-assertion|authentication/oauth2/assertion/api_oauth2_assertion_source.py|8081|authentication/oauth2/assertion/api_oauth2_assertion_etl.py|no|requires certs (auto-generated via generate_assertion_certs.sh)
auth-oauth2-authorization-code|authentication/oauth2/authorization_code/api_oauth2_authorization_code_source.py|8081|authentication/oauth2/authorization_code/authorization_code_client.py|yes|opens a browser for the auth-code flow; requires GITHUB_CLIENT_ID/GITHUB_CLIENT_SECRET env vars
auth-oauth2-device-code|-|-|authentication/oauth2/device_code/device_code_dummy.py|yes|opens a browser and requires a GitHub device-code login; requires GITHUB_CLIENT_ID env var
auth-oauth2-authlib-client-cred|-|-|authentication/oauth2/client_credential/authlib_client_cred.py|yes|requires GITHUB_CLIENT_ID/SECRET env vars
paginated-cursor|paginated/cursor/cursor_pagination_source.py|8081|paginated/cursor/paginated_cursor_etl.py|no|
paginated-offset-simple|paginated/offset/simple/simple_offset_source.py|8081|paginated/offset/simple/simple_offset_etl.py|no|
paginated-offset-page-token|paginated/offset/page_token/offset_page_token_source.py|8081|paginated/offset/page_token/offset_page_token_etl.py|no|
paginated-page-number-hn|paginated/page_number/paginated_page_number_hn_source.py|8081|paginated/page_number/paginated_page_number_hn_etl.py|no|
paginated-page-number-ti|paginated/page_number/paginated_page_number_ti_source.py|8081|paginated/page_number/paginated_page_number_ti_etl.py|no|
paginated-page-number-tp|paginated/page_number/paginated_page_number_tp_source.py|8081|paginated/page_number/paginated_page_number_tp_etl.py|no|
incremental|incremental/mock_incremental_server.py|8090|incremental/run_incremental_example.py|no|
ingestion-parallel|ingestion/mock_items_server.py|8091|ingestion/parallel_ingestion.py|no|reuses the shared /items mock server
ingestion-parallel-page|ingestion/mock_items_server.py|8091|ingestion/parallel_ingestion_page.py|no|reuses the shared /items mock server
ingestion-parallel-partitions|ingestion/mock_items_server.py|8091|ingestion/parallel_with_spark_partitions.py|no|reuses the shared /items mock server
ingestion-pyspark-rest-api|ingestion/mock_items_server.py|8091|ingestion/pyspark_rest_api.py|no|reuses the shared /items mock server
ingestion-pyspark-rest-optimized|ingestion/mock_items_server.py|8091|ingestion/pyspark_rest_optimized.py|no|reuses the shared /items mock server
config-yaml|ingestion/mock_items_server.py|8091|config/yaml_config_api.py|no|reuses the shared /items mock server
schema-demo|-|-|schema/demo_json_schema.py|no|
util-main|-|-|util/main.py|no|hits the live openweathermap API
"

log()  { printf '\033[1;34m[run-examples]\033[0m %s\n' "$*"; }
# NOTE: warn/err intentionally write to stdout, not stderr. Spawning a
# PySpark/JVM child in this repo's mock-server + ETL scenarios has been
# observed to leave the parent shell's stderr undeliverable for the rest of
# the run in some sandboxes, silently swallowing later warnings/errors.
# stdout has proven reliable across every scenario, so all status output
# (including failures) goes there to guarantee it's never lost.
warn() { printf '\033[1;33m[run-examples]\033[0m %s\n' "$*"; }
err()  { printf '\033[1;31m[run-examples]\033[0m %s\n' "$*"; }

usage() {
    sed -n '2,16p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

scenario_row() {
    local name="$1"
    echo "${SCENARIOS}" | awk -F'|' -v n="${name}" 'NF && $1 == n {print; found=1} END {exit !found}'
}

list_scenarios() {
    printf '%-42s %-8s %s\n' "NAME" "MANUAL" "NOTE"
    echo "${SCENARIOS}" | awk -F'|' 'NF {printf "%-42s %-8s %s\n", $1, $5, $6}'
}

wait_for_port() {
    local port="$1" timeout="$2" waited=0
    while ! (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null; do
        exec 3<&- 2>/dev/null || true
        sleep 1
        waited=$((waited + 1))
        if [ "${waited}" -ge "${timeout}" ]; then
            return 1
        fi
    done
    exec 3<&- 2>/dev/null || true
    return 0
}

run_scenario() {
    local name="$1"
    shift
    local extra_args=("$@")

    local row
    if ! row="$(scenario_row "${name}")"; then
        err "Unknown scenario: ${name}"
        list_scenarios
        return 1
    fi

    IFS='|' read -r _ server_script port etl_script manual note <<<"${row}"

    if [ "${name}" = "auth-mtls" ] && [ ! -d "${DATA_HOME:-/tmp}/rest_api_ds/certs" ]; then
        log "Generating mTLS certs (one-time setup)..."
        (cd "${REPO_ROOT}/examples/authentication/mtls" && bash generate_mtls_certs.sh)
    fi

    if [ "${name}" = "auth-oauth2-assertion" ] && [ ! -f "${DATA_HOME:-/tmp}/rest_api_ds/certs/assertion_client.pem" ]; then
        log "Generating oauth2 assertion signing certs (one-time setup)..."
        (cd "${REPO_ROOT}/examples/authentication/oauth2/assertion" && bash generate_assertion_certs.sh)
    fi

    local server_pid=""
    local server_log="${LOG_DIR}/${name}.server.log"

    if [ "${server_script}" != "-" ]; then
        mkdir -p "${LOG_DIR}"
        : >"${server_log}"
        log "Starting mock server: ${server_script} (port ${port}, log: ${server_log})"
        (cd "${REPO_ROOT}" && PYTHONPATH=src uv run python "examples/${server_script}") >"${server_log}" 2>&1 &
        server_pid=$!

        if ! wait_for_port "${port}" "${DEFAULT_PORT_READY_TIMEOUT}"; then
            err "Server for '${name}' did not become ready on port ${port} within ${DEFAULT_PORT_READY_TIMEOUT}s"
            tail -n 20 "${server_log}" || true
            kill "${server_pid}" 2>/dev/null || true
            wait "${server_pid}" 2>/dev/null || true
            return 1
        fi
        log "Server ready on port ${port} (pid ${server_pid})"
    fi

    log "Running: ${etl_script}${note:+  # ${note}}"
    local etl_status=0
    (
        cd "${REPO_ROOT}"
        PYTHONPATH=src timeout "${DEFAULT_ETL_TIMEOUT}" uv run python "examples/${etl_script}" "${extra_args[@]}"
    ) || etl_status=$?

    if [ -n "${server_pid}" ] && kill -0 "${server_pid}" 2>/dev/null; then
        kill "${server_pid}" 2>/dev/null || true
        wait "${server_pid}" 2>/dev/null || true
    fi

    return "${etl_status}"
}

main() {
    local cmd="${1:-}"
    case "${cmd}" in
        list|"")
            list_scenarios
            ;;
        run)
            local name="${2:-}"
            [ -z "${name}" ] && { err "Usage: $0 run <name> [-- extra args]"; exit 1; }
            shift 2 || true
            [ "${1:-}" = "--" ] && shift
            run_scenario "${name}" "$@"
            ;;
        all)
            local include_manual="no"
            [ "${2:-}" = "--include-manual" ] && include_manual="yes"

            local total=0 passed=0 failed=0 skipped=0
            local failed_names=()
            while IFS='|' read -r name server_script port etl_script manual note; do
                [ -z "${name}" ] && continue
                total=$((total + 1))
                if [ "${manual}" = "yes" ] && [ "${include_manual}" != "yes" ]; then
                    warn "SKIP  ${name}  (manual — needs interaction/credentials)"
                    skipped=$((skipped + 1))
                    continue
                fi
                if run_scenario "${name}"; then
                    log "PASS  ${name}"
                    passed=$((passed + 1))
                else
                    err "FAIL  ${name}"
                    failed=$((failed + 1))
                    failed_names+=("${name}")
                fi
            done <<<"${SCENARIOS}"

            echo
            log "Summary: ${passed} passed, ${failed} failed, ${skipped} skipped, ${total} total"
            if [ "${failed}" -gt 0 ]; then
                err "Failed scenarios: ${failed_names[*]}"
                exit 1
            fi
            ;;
        -h|--help|help)
            usage
            ;;
        *)
            err "Unknown command: ${cmd}"
            usage
            exit 1
            ;;
    esac
}

main "$@"
