#!/usr/bin/env bash
# CLI wrapper for the durable pipeline scheduler.
#
# Schedules run pipeline jobs on a cron/interval, remember the "last good
# point" (checkpoint) between runs, and catch up automatically after the
# driver has been offline. See docs/scheduling.md in hitorro-mesh-pipelines
# for the concept guide.
#
# Usage:
#   mesh-schedule.sh list
#   mesh-schedule.sh get <name>
#   mesh-schedule.sh create <name> <job.yaml> [--cron '0 0 * * * *' | --every 3600s]
#                                             [--catchup-grace 300s]
#                                             [--max-concurrent 1]
#                                             [--disabled]
#   mesh-schedule.sh update <name> <job.yaml> [same flags]
#   mesh-schedule.sh delete <name>
#   mesh-schedule.sh pause  <name>
#   mesh-schedule.sh resume <name>
#   mesh-schedule.sh run-now <name>              # trigger immediately, honours cap
#   mesh-schedule.sh checkpoint get <name>
#   mesh-schedule.sh checkpoint set <name> <value>
#
# Job templates receive the current checkpoint via ${CHECKPOINT} — see
# docs/examples/mac-mail-hourly-schedule.yaml for a worked example.
set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh

BASE_URL="http://localhost:${MESH_DRIVER_PORT}"
PRETTY=0
CRON=""
EVERY=""
GRACE=""
MAX_CONC=""
ENABLED=true

_die() { echo "$*" >&2; exit 2; }
_out() { if [ "$PRETTY" = 1 ] && command -v jq >/dev/null 2>&1; then jq .; else cat; fi; }

_parse_secs() {
    # accepts "300", "300s", "5m", "1h"
    local v=$1
    case "$v" in
        *s) echo "${v%s}" ;;
        *m) echo $(( ${v%m} * 60 )) ;;
        *h) echo $(( ${v%h} * 3600 )) ;;
        *)  echo "$v" ;;
    esac
}

_parse_flags() {
    _rest=()
    while [ $# -gt 0 ]; do
        case "$1" in
            --pretty)         PRETTY=1 ;;
            --cron)           shift; CRON=$1 ;;
            --every)          shift; EVERY=$(_parse_secs "$1") ;;
            --catchup-grace)  shift; GRACE=$(_parse_secs "$1") ;;
            --max-concurrent) shift; MAX_CONC=$1 ;;
            --disabled)       ENABLED=false ;;
            *)                _rest+=("$1") ;;
        esac
        shift
    done
}

_build_body() {
    # Args: name, yaml_file. Emits schedule JSON on stdout.
    local name=$1 yaml=$2
    [ -f "$yaml" ] || _die "not a file: $yaml"
    python3 - "$name" "$yaml" "$CRON" "$EVERY" "$GRACE" "$MAX_CONC" "$ENABLED" <<'PY'
import json, sys
name, path, cron, every, grace, maxc, enabled = sys.argv[1:8]
body = { "name": name, "jobYaml": open(path).read(), "enabled": enabled == "true" }
if cron:  body["cron"] = cron
if every: body["intervalSeconds"] = int(every)
if grace: body["catchupGraceSeconds"] = int(grace)
if maxc:  body["maxConcurrent"] = int(maxc)
if not cron and not every:
    print("must specify --cron or --every", file=sys.stderr); sys.exit(2)
print(json.dumps(body))
PY
}

_cmd_list()  { curl -sS "$BASE_URL/mesh/schedules" | _out; }
_cmd_get()   { curl -sS "$BASE_URL/mesh/schedules/${_rest[0]:?name required}" | _out; }
_cmd_del()   { curl -sS -X DELETE "$BASE_URL/mesh/schedules/${_rest[0]:?name required}" | _out; }
_cmd_pause() { curl -sS -X POST "$BASE_URL/mesh/schedules/${_rest[0]:?name required}/pause" | _out; }
_cmd_resume(){ curl -sS -X POST "$BASE_URL/mesh/schedules/${_rest[0]:?name required}/resume" | _out; }
_cmd_run()   { curl -sS -X POST "$BASE_URL/mesh/schedules/${_rest[0]:?name required}/run-now" | _out; }

_cmd_create() {
    local body; body=$(_build_body "${_rest[0]:?name}" "${_rest[1]:?job.yaml}")
    curl -sS -X POST "$BASE_URL/mesh/schedules" -H 'content-type: application/json' -d "$body" | _out
}

_cmd_update() {
    # Same shape as create — PUT semantics via POST-replace on the same name.
    _cmd_create
}

_cmd_checkpoint() {
    local action=${_rest[0]:?get|set}
    local name=${_rest[1]:?name}
    case "$action" in
        get) curl -sS "$BASE_URL/mesh/schedules/$name/checkpoint" | _out ;;
        set)
            local val=${_rest[2]:?checkpoint value}
            curl -sS -X PUT "$BASE_URL/mesh/schedules/$name/checkpoint" \
                 -H 'content-type: text/plain' --data-binary "$val" | _out ;;
        *) _die "usage: checkpoint <get|set> <name> [value]" ;;
    esac
}

sub=${1:-}
[ -n "$sub" ] || _die "usage: $0 <list|get|create|update|delete|pause|resume|run-now|checkpoint> [args]"
shift
_parse_flags "$@"
case "$sub" in
    list)        _cmd_list ;;
    get)         _cmd_get ;;
    create)      _cmd_create ;;
    update)      _cmd_update ;;
    delete|rm)   _cmd_del ;;
    pause)       _cmd_pause ;;
    resume)      _cmd_resume ;;
    run-now)     _cmd_run ;;
    checkpoint)  _cmd_checkpoint ;;
    *)           _die "unknown subcommand: $sub" ;;
esac
