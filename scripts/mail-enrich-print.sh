#!/usr/bin/env bash
# =============================================================================
#  mail-enrich-print.sh
#
#  END-TO-END EXAMPLE #1 — SQL against Mail → JVS enrichment → print JSON.
#
#  What it does, top-to-bottom:
#    1. Runs a SQLite query against your local Apple Mail Envelope Index.
#    2. Reshapes each row into a JVS `mail_email` document (title/body as
#       core_mls so enrichment has somewhere to write).
#    3. Runs `jvs-enrich` with the [basic, segmented, pos, ner] tags —
#       populates .clean / .segmented / .pos / .segmented_ner on every
#       core_mls entry.
#    4. Writes enriched rows to a temp NDJSON file.
#    5. Prints them, pretty-formatted, to stdout so you can see the shape
#       (subject.segmented_ner, body.segmented_ner in the canonical
#       bracket format `[{token&&NE_Type}]`).
#
#  Usage:
#     ./mail-enrich-print.sh                       # 5 rows, default
#     ./mail-enrich-print.sh --limit 20            # 20 rows
#     ./mail-enrich-print.sh --where "addr.address LIKE '%github.com'"
#     ./mail-enrich-print.sh --limit 3 --raw       # NDJSON one line per row
#
#  Requires: mesh running (mesh-up.sh), macOS Mail with V10 database.
# =============================================================================
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
MESH_PIPELINE=${MESH_PIPELINE:-"$HERE/mesh-pipeline.sh"}

LIMIT=5
WHERE="1=1"
RAW=0
while [ $# -gt 0 ]; do
    case "$1" in
        --limit) LIMIT=$2; shift 2 ;;
        --where) WHERE=$2; shift 2 ;;
        --raw)   RAW=1;    shift   ;;
        -h|--help)
            sed -n '2,25p' "$0"; exit 0 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

OUT=$(mktemp -t hitorro-mail-enriched.XXXXXX).ndjson
JOB=$(mktemp -t hitorro-mail-job.XXXXXX).yaml
trap 'rm -f "$OUT" "$JOB"' EXIT

# ---------------------------------------------------------------------------
#  Pipeline definition — inline YAML. The SQL is right here so you can see
#  exactly what data feeds the enrichment.
# ---------------------------------------------------------------------------
cat > "$JOB" <<YAML
job: mail-enrich-print
nodes:
  - id: enrich
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
            summ.summary     AS body
          FROM messages   m
          LEFT JOIN addresses addr ON addr.ROWID = m.sender
          LEFT JOIN subjects  subj ON subj.ROWID = m.subject
          LEFT JOIN summaries summ ON summ.ROWID = m.summary
          WHERE subj.subject IS NOT NULL
            AND summ.summary IS NOT NULL
            AND ($WHERE)
          ORDER BY m.date_received DESC
          LIMIT $LIMIT
      steps:
        # Reshape flat SQL row → JVS mail_email tree.
        # NOTE on id: sysobject declares id as core_id — a composite
        # {domain, did, id} — NOT a scalar. Setting out.id to a bare
        # string trips "PropaccessError: object node expected id.id"
        # during Lucene projection. Wrap it as {id, did, domain}.
        - kind: groovy-map
          script: |
            def sid = String.valueOf(row.id)
            def out = [ht_type: 'mail_email', id: [id: sid, did: sid, domain: 'mail']]
            out.title = [mls: [[lang: 'en', text: row.subject ?: '']]]
            if (row.body != null && !row.body.toString().isEmpty()) {
                out.body = [mls: [[lang: 'en', text: row.body]]]
            }
            out.sender_address = row.sender_address
            out.sender_name    = row.sender_name
            if (row.received_ts != null) {
                out.times = [date_received: (long)(row.received_ts * 1000L)]
            }
            out
        # Run enrichment — title + body get sentence-segmented + POS-tagged
        # + NER-marked with [{token&&NE_Type}] bracket format.
        - kind: jvs-enrich
          tags: [basic, segmented, pos, ner]
          typeJsonResource: "classpath:/types/mail_email.json"
      sinks:
        - kind: ndjson-file
          url: "file://$OUT"
YAML

echo "--- running pipeline (limit=$LIMIT where=$WHERE) ---" >&2
"$MESH_PIPELINE" run "$JOB" --wait --json | jq -r '.state + " (" + (.nodes[0].rowsOut|tostring) + " rows)"' >&2

if [ "$RAW" = 1 ]; then
    cat "$OUT"
    exit 0
fi

# ---------------------------------------------------------------------------
#  Pretty-print — one summary block per enriched email showing the two
#  enrichment outputs you care about: the sentence list + the bracket-
#  format NER markup that feeds the Lucene indexer.
# ---------------------------------------------------------------------------
jq -r '
  "=========================================================",
  "id:       " + (.id.id // .id.did // "?"),
  "sender:   " + (.sender_address // "(none)"),
  "subject:  " + (.title.mls[0].text // ""),
  "",
  "title.segmented_ner:",
  (.title.mls[0].segmented_ner // [] | map("  · " + .) | .[]),
  "",
  "body.segmented (first 3 sentences):",
  (.body.mls[0].segmented // [] | .[0:3] | map("  " + .) | .[]),
  "",
  "body.segmented_ner (first 3 sentences, bracket format):",
  (.body.mls[0].segmented_ner // [] | .[0:3] | map("  " + .) | .[]),
  ""
' "$OUT"
