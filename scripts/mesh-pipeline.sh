#!/usr/bin/env bash
# Shell CLI for the pipelines REST surface. Pipe-friendly output
# (single-line JSON per response by default; --pretty for jq-style).
#
# Usage:
#   mesh-pipeline.sh run <file.yaml> [--distributed] [--wait] [--events]
#   mesh-pipeline.sh run-bundled <name> [--distributed] [--wait]
#   mesh-pipeline.sh run-groovy <file.groovy> [--distributed] [--wait]
#   mesh-pipeline.sh status <jobId>
#   mesh-pipeline.sh events <jobId>
#   mesh-pipeline.sh cancel <jobId>
#   mesh-pipeline.sh list [--limit N] [--running-only]
#   mesh-pipeline.sh bundled
#   mesh-pipeline.sh tables            # registered runtime tables
#   mesh-pipeline.sh query "SELECT ..."
#
# Flags:
#   --pretty         run every JSON response through `jq .` if jq is installed
#   --wait           poll until the job reaches a terminal state
#   --events         with --wait, also stream progress events to stderr
#   --distributed    hit /run-distributed (rejects sqlite sources — driver-
#                    local only for those)
#   --json           JSON output only, no headers (default when stdout is
#                    a pipe; useful for `mesh-pipeline.sh status $JID | jq`)
#
# Examples:
#   ./mesh-pipeline.sh run mail-register.yaml --wait
#   ./mesh-pipeline.sh query "SELECT sender_domain_folded, COUNT(*) AS n \
#       FROM mail_messages GROUP BY sender_domain_folded ORDER BY n DESC LIMIT 15" --pretty
#   ./mesh-pipeline.sh list --running-only --json | jq -r '.[].jobId'
#   JID=$(./mesh-pipeline.sh run tiny.yaml | jq -r .jobId)
#   ./mesh-pipeline.sh events $JID | grep progress
#
# Exit codes:
#   0 = success (job reached SUCCEEDED with --wait, or REST call returned 2xx)
#   1 = job reached FAILED/CANCELLED terminal state with --wait
#   2 = usage error
#   3 = REST error (non-2xx response)
set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh

BASE_URL="http://localhost:${MESH_DRIVER_PORT}"

PRETTY=0
WAIT=0
EVENTS=0
DISTRIBUTED=0
JSON_ONLY=0
LIMIT=20
RUNNING_ONLY=0

# Auto-detect: piped stdout → JSON-only (no decorative headers).
[ -t 1 ] || JSON_ONLY=1

_die() { echo "$*" >&2; exit 2; }
_out() { if [ "$PRETTY" = 1 ] && command -v jq >/dev/null 2>&1; then jq .; else cat; fi; }
_header() { [ "$JSON_ONLY" = 1 ] || echo "$*" >&2; }

# Parse global flags out of "$@" — positional args remain in _remaining.
_parse_flags() {
    _remaining=()
    while [ $# -gt 0 ]; do
        case "$1" in
            --pretty)        PRETTY=1        ;;
            --wait)          WAIT=1          ;;
            --events)        EVENTS=1        ;;
            --distributed)   DISTRIBUTED=1   ;;
            --json)          JSON_ONLY=1     ;;
            --running-only)  RUNNING_ONLY=1  ;;
            --limit)         shift; LIMIT="$1" ;;
            --)              shift; while [ $# -gt 0 ]; do _remaining+=("$1"); shift; done; return ;;
            -*)              _die "unknown flag: $1" ;;
            *)               _remaining+=("$1") ;;
        esac
        shift
    done
}

# Wait for a job — poll status every second, print terminal state to stderr,
# echo the final snapshot to stdout. --events streams progress to stderr as
# they arrive.
_wait_job() {
    local jid=$1
    _header "-> polling $jid..."
    local last_event_count=0
    while true; do
        local snap; snap=$(curl -sS "$BASE_URL/mesh/jobs/$jid")
        local state; state=$(echo "$snap" | python3 -c "import json,sys; print(json.load(sys.stdin)['state'])")
        if [ "$EVENTS" = 1 ]; then
            local events; events=$(curl -sS "$BASE_URL/mesh/jobs/$jid/events")
            local n; n=$(echo "$events" | python3 -c "import json,sys; print(len(json.load(sys.stdin)))")
            if [ "$n" -gt "$last_event_count" ]; then
                echo "$events" | python3 -c "
import json,sys
evs = json.load(sys.stdin)
for e in evs[$last_event_count:]:
    print(f\"  [{e['at'][11:19]}] {e['nodeId']:>10s} {e['kind']:>10s}: {e['message']}\", file=sys.stderr)
"
                last_event_count=$n
            fi
        fi
        if [ "$state" != "RUNNING" ] && [ "$state" != "PENDING" ]; then
            _header "= $jid: $state"
            echo "$snap" | _out
            [ "$state" = "SUCCEEDED" ] && return 0 || return 1
        fi
        sleep 1
    done
}

# Submit a body (YAML/JSON/Groovy) — echoes {jobId} or waits for terminal.
_submit() {
    local endpoint=$1 content_type=$2 body=$3
    local resp; resp=$(curl -sS -X POST "$BASE_URL$endpoint" \
        -H "content-type: $content_type" \
        --data-binary "$body")
    if [ "$WAIT" = 1 ]; then
        local jid; jid=$(echo "$resp" | python3 -c "import json,sys; print(json.load(sys.stdin).get('jobId',''))")
        if [ -z "$jid" ]; then
            _header "submit failed:"
            echo "$resp" | _out
            exit 3
        fi
        _wait_job "$jid"
    else
        echo "$resp" | _out
    fi
}

_cmd_run() {
    local file=${_remaining[0]:-}
    [ -f "$file" ] || _die "usage: run <file.yaml> - file not found: $file"
    local endpoint=/mesh/jobs/run
    [ "$DISTRIBUTED" = 1 ] && endpoint=/mesh/jobs/run-distributed
    _submit "$endpoint" application/x-yaml "@$file"
}

_cmd_run_bundled() {
    local name=${_remaining[0]:-}
    [ -n "$name" ] || _die "usage: run-bundled <name>"
    _submit "/mesh/jobs/run/bundled/$name" application/x-yaml ""
}

_cmd_run_groovy() {
    local file=${_remaining[0]:-}
    [ -f "$file" ] || _die "usage: run-groovy <file.groovy> - file not found: $file"
    _submit /mesh/jobs/run-groovy application/groovy "@$file"
}

_cmd_status() {
    local jid=${_remaining[0]:-}
    [ -n "$jid" ] || _die "usage: status <jobId>"
    curl -sS "$BASE_URL/mesh/jobs/$jid" | _out
}

_cmd_events() {
    local jid=${_remaining[0]:-}
    [ -n "$jid" ] || _die "usage: events <jobId>"
    curl -sS "$BASE_URL/mesh/jobs/$jid/events" | _out
}

_cmd_cancel() {
    local jid=${_remaining[0]:-}
    [ -n "$jid" ] || _die "usage: cancel <jobId>"
    curl -sS -X DELETE "$BASE_URL/mesh/jobs/$jid" | _out
}

_cmd_list() {
    curl -sS "$BASE_URL/mesh/jobs" | python3 -c "
import json,sys
d = json.load(sys.stdin)
if $RUNNING_ONLY: d = [j for j in d if j['state']=='RUNNING']
print(json.dumps(d[:$LIMIT]))
" | _out
}

_cmd_bundled() {
    curl -sS "$BASE_URL/mesh/jobs/bundled" | _out
}

_cmd_tables() {
    curl -sS "$BASE_URL/mesh/queries/registered" | _out
}

_cmd_query() {
    local sql=${_remaining[0]:-}
    [ -n "$sql" ] || _die "usage: query \"<SQL>\""
    local body; body=$(python3 -c "import json,sys; print(json.dumps({'sql': sys.argv[1]}))" "$sql")
    curl -sS -X POST "$BASE_URL/mesh/queries" \
         -H 'content-type: application/json' \
         -d "$body" | _out
}

# ------------------------------------------------------------------------
sub=${1:-}
[ -n "$sub" ] || _die "usage: $0 <run|run-bundled|run-groovy|status|events|cancel|list|bundled|tables|query> [args] [flags]"
shift
_parse_flags "$@"
case "$sub" in
    run)          _cmd_run          ;;
    run-bundled)  _cmd_run_bundled  ;;
    run-groovy)   _cmd_run_groovy   ;;
    status)       _cmd_status       ;;
    events)       _cmd_events       ;;
    cancel)       _cmd_cancel       ;;
    list)         _cmd_list         ;;
    bundled)      _cmd_bundled      ;;
    tables)       _cmd_tables       ;;
    query)        _cmd_query        ;;
    *) _die "unknown subcommand: $sub - run without args for usage" ;;
esac
