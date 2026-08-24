#!/usr/bin/env bash
# =============================================================================
#  mail-enrich-search.sh
#
#  END-TO-END EXAMPLE #2 — SQL against Mail → JVS enrichment → sink to BOTH
#  a type-aware Lucene index AND a RocksDB KV store → run searches back
#  against the index.
#
#  Pipeline shape:
#
#         sqlite ──► groovy-map ──► jvs-enrich ──┬─► jvs-lucene sink
#                                                └─► kvstore   sink
#
#  Why both sinks?
#    * jvs-lucene indexes every field per its JVS type's projection
#      strategy (analyzers per language, StringField for identifiers,
#      textmarkup analyzer for segmented_ner bracket tokens, etc.).
#      Optimal for search/discovery.
#    * kvstore keeps the FULL enriched JVS keyed by id — cheap
#      exact-id retrieval, downstream jobs can consume via
#      `source: {kind: kvstore, name: mail-enriched-kv}` without
#      re-running enrichment.
#
#  After the pipeline finishes, this script runs a couple of demo
#  searches against the fresh Lucene index to prove the round trip.
#
#  Usage:
#     ./mail-enrich-search.sh                          # 200 rows, demo searches
#     ./mail-enrich-search.sh --limit 500              # bigger corpus
#     ./mail-enrich-search.sh --limit 50 --wipe        # start clean
#     ./mail-enrich-search.sh --skip-run --search NE_Person   # skip pipeline, search only
#     ./mail-enrich-search.sh --search 'body.mls.clean:kubernetes'
#
#  Requires: mesh running, driver at localhost:8085.
# =============================================================================
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
MESH_PIPELINE=${MESH_PIPELINE:-"$HERE/mesh-pipeline.sh"}
DRIVER=${DRIVER:-http://localhost:8085}

INDEX_NAME=mail-enriched-idx        # Lucene index name (dir under ~/.hitorro/pipelines/lucene/)
KV_NAME=mail-enriched-kv            # KV store name (dir under ~/.hitorro/pipelines/kv/)
LUCENE_HOME=$HOME/.hitorro/pipelines/lucene/$INDEX_NAME
KV_HOME=$HOME/.hitorro/pipelines/kv/$KV_NAME

LIMIT=200
WHERE="1=1"
SKIP_RUN=0
WIPE=0
SEARCHES=()
while [ $# -gt 0 ]; do
    case "$1" in
        --limit)     LIMIT=$2;   shift 2 ;;
        --where)     WHERE=$2;   shift 2 ;;
        --wipe)      WIPE=1;     shift   ;;
        --skip-run)  SKIP_RUN=1; shift   ;;
        --search)    SEARCHES+=("$2"); shift 2 ;;
        -h|--help)   sed -n '2,32p' "$0"; exit 0 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

# Default search set that exercises every field projection kind — swap
# in your own via --search 'field:value' (repeatable). Field paths are
# the LOGICAL JVS paths; the search service uses the JVS type sidecar
# written by the jvs-lucene sink to expand them to the underlying
# Lucene projected names (e.g. body.mls.segmented_ner →
# body.mls.segmented_ner.textmarkup_en_m). Physical names still parse
# unchanged for backward compat.
if [ ${#SEARCHES[@]} -eq 0 ]; then
    SEARCHES=(
        'body.mls.segmented_ner:NE_Person'          # NER type filter
        'body.mls.segmented_ner:NE_Location'
        'body.mls.clean:meeting'                    # full-text over cleaned body
        'title.mls.clean:invoice'                   # full-text over subject
    )
fi

# ---------------------------------------------------------------------------
#  Optional wipe — remove the old index + KV so this run starts clean.
#  Necessary if you changed the pipeline shape (added/removed fields)
#  and the old segment layout is incompatible.
# ---------------------------------------------------------------------------
if [ $WIPE = 1 ]; then
    echo "--- wiping $LUCENE_HOME + $KV_HOME ---" >&2
    rm -rf "$LUCENE_HOME" "$KV_HOME"
fi

# ---------------------------------------------------------------------------
#  RUN — inline YAML pipeline (edit the SQL + WHERE right here).
# ---------------------------------------------------------------------------
if [ $SKIP_RUN = 0 ]; then
    JOB=$(mktemp -t hitorro-mail-search.XXXXXX).yaml
    trap 'rm -f "$JOB"' EXIT

    cat > "$JOB" <<YAML
job: mail-enrich-search
nodes:
  - id: enrich-and-index
    pipeline:
      source:
        kind: sqlite
        path: "~/Library/Mail/V10/MailData/Envelope Index"
        query: |
          SELECT
            m.ROWID          AS id,
            m.date_received  AS received_ts,
            addr.address     AS sender_address,
            addr.comment     AS sender_name,
            subj.subject     AS subject,
            summ.summary     AS body,
            mb.url           AS mailbox_url,
            m.read           AS read,
            m.flagged        AS flagged,
            m.size           AS size_bytes
          FROM messages   m
          LEFT JOIN addresses addr ON addr.ROWID = m.sender
          LEFT JOIN subjects  subj ON subj.ROWID = m.subject
          LEFT JOIN summaries summ ON summ.ROWID = m.summary
          LEFT JOIN mailboxes mb   ON mb.ROWID   = m.mailbox
          WHERE subj.subject IS NOT NULL
            AND summ.summary IS NOT NULL
            AND ($WHERE)
          ORDER BY m.date_received DESC
          LIMIT $LIMIT
      steps:
        - kind: groovy-map
          script: |
            // id is core_id (composite {domain, did, id}) via sysobject —
            // MUST be an object, not a scalar, or the Lucene projection
            // throws "PropaccessError: object node expected id.id".
            def sid = String.valueOf(row.id)
            def out = [ht_type: 'mail_email', id: [id: sid, did: sid, domain: 'mail']]
            out.title = [mls: [[lang: 'en', text: row.subject ?: '']]]
            if (row.body != null && !row.body.toString().isEmpty()) {
                out.body = [mls: [[lang: 'en', text: row.body]]]
            }
            def dom = null
            if (row.sender_address != null) {
                def at = row.sender_address.indexOf('@')
                if (at > 0) dom = row.sender_address.substring(at + 1).toLowerCase()
            }
            out.sender_address = row.sender_address
            out.sender_name    = row.sender_name
            out.sender_domain  = dom
            out.mailbox_url    = row.mailbox_url
            out.read           = row.read == 1
            out.flagged        = row.flagged == 1
            out.size_bytes     = row.size_bytes
            if (row.received_ts != null) {
                out.times = [date_received: (long)(row.received_ts * 1000L)]
            }
            out
        - kind: jvs-enrich
          tags: [basic, segmented, pos, ner]
          typeJsonResource: "classpath:/types/mail_email.json"
      sinks:
        # SINK 1 — type-aware Lucene index. Every field routed to an
        # analyzer chosen by the JVS type's groups[].method. Search via
        # GET $DRIVER/mesh/search/$INDEX_NAME?q=<field>:<value>.
        - kind: jvs-lucene
          name: $INDEX_NAME
          typeJsonResource: "classpath:/types/mail_email.json"
        # SINK 2 — RocksDB KV store, keyed by row.id. The full enriched
        # JSON becomes the value. Retrievable via
        # $DRIVER/mesh/kv/$KV_NAME/<id>.
        - kind: kvstore
          name: $KV_NAME
          # keyExpr must resolve to a scalar. Our reshape sets id as a
          # composite core_id {id, did, domain}, so navigate to id.id
          # for the string key. Using bare "id" here silently stored
          # every row under the empty-string key (last-write-wins).
          keyExpr: id.id
YAML

    echo "--- running pipeline (limit=$LIMIT where=$WHERE) → lucene=$INDEX_NAME kv=$KV_NAME ---" >&2
    "$MESH_PIPELINE" run "$JOB" --wait --json \
        | jq -r '"state=" + .state + " rows=" + (.nodes[0].rowsOut|tostring) + " sinks=" + (.nodes[0].sinkCounts|tostring)' >&2
fi

# ---------------------------------------------------------------------------
#  DEMO SEARCHES — one Lucene query per SEARCHES entry.
# ---------------------------------------------------------------------------
echo >&2
echo "==========================================================" >&2
echo " SEARCH RESULTS — index=$INDEX_NAME" >&2
echo "==========================================================" >&2

for q in "${SEARCHES[@]}"; do
    enc=$(printf '%s' "$q" | python3 -c 'import sys,urllib.parse;print(urllib.parse.quote(sys.stdin.read()))')
    echo >&2
    echo "--- q=$q ---" >&2
    curl -sS "$DRIVER/mesh/search/$INDEX_NAME?q=$enc&max=3" \
        | jq -r --arg q "$q" '
              "  totalDocsInIndex: \(.totalDocsInIndex)  hitCount: \(.hitCount)",
              "  top hits:",
              (.hits // [] | map("    · id=\(.id)  from=\(.sender_address // "?")  subject=\(.title.mls[0].text // "?")") | .[])
          '
done

# ---------------------------------------------------------------------------
#  KV VERIFICATION — the RocksDB directory should be populated with one
#  entry per row we ingested. There's no built-in HTTP endpoint to fetch
#  a value out yet (candidate for a small future PipelineKvController);
#  downstream jobs consume it via `source: {kind: kvstore, name: $KV_NAME}`
#  which runs in-process on the driver/agent. Meanwhile prove the sink
#  produced real bytes on disk.
# ---------------------------------------------------------------------------
echo >&2
echo "==========================================================" >&2
echo " KV STORE — $KV_HOME" >&2
echo "==========================================================" >&2
if [ -d "$KV_HOME" ]; then
    bytes=$(du -sk "$KV_HOME" 2>/dev/null | awk '{print $1}')
    files=$(find "$KV_HOME" -type f 2>/dev/null | wc -l | awk '{print $1}')
    echo "  $files files, ${bytes}K on disk"
    echo "  layout:"
    find "$KV_HOME" -maxdepth 2 -type f 2>/dev/null | sed 's|^|    |'
    echo
    echo "  Consume from another pipeline node with:"
    echo "    source: {kind: kvstore, name: $KV_NAME}"
else
    echo "  no directory at $KV_HOME — sink may have failed, check pipeline status"
fi
