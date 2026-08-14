/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.examples;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.mesh.InMemoryMeshTransport;
import com.hitorro.mesh.MeshTransport;
import com.hitorro.mesh.agent.AgentConfig;
import com.hitorro.mesh.agent.LocalTable;
import com.hitorro.mesh.agent.MeshAgent;
import com.hitorro.mesh.driver.DistributedTable;
import com.hitorro.mesh.driver.DistributedTableRegistry;
import com.hitorro.mesh.driver.MeshDriver;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 4b: shuffle-hash JOIN between two distributed tables.
 *
 * <p>Cluster shape: 4 agents. Two hold {@code docs} partitions (us, eu),
 * two hold {@code events} partitions (us, eu). Each event references a
 * doc by ID. The join partitions both sides by their join key into N
 * shuffle buckets; per-bucket combine workers do the actual join.</p>
 */
class ShuffleJoinTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void innerJoin_twoDistributedTables_returnsMatchingPairs() throws Exception {
        try (var c = new TestCluster(/*shuffleWidth*/ 2)) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT docs.id, docs.title, events.action FROM docs " +
                    "JOIN events ON docs.id = events.doc_id")) {
                var rows = h.collect(15, TimeUnit.SECONDS);
                // Every event has a matching doc (see data below), so we get
                // one output row per event.
                assertThat(rows).hasSize(5);
                // Spot-check the join preserved the associations.
                Map<String, String> idToAction = new HashMap<>();
                for (JsonNode r : rows) {
                    idToAction.put(r.get("id").asText() + ":" + r.get("action").asText(),
                            r.get("title").asText());
                }
                assertThat(idToAction).containsKey("us-1:view");
                assertThat(idToAction).containsKey("eu-1:download");
            }
        }
    }

    @Test
    void innerJoin_widerShuffle_stillCorrect() throws Exception {
        // Same query, wider shuffle (4 buckets). Same expected result.
        try (var c = new TestCluster(/*shuffleWidth*/ 4)) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT docs.id, events.action FROM docs " +
                    "JOIN events ON docs.id = events.doc_id")) {
                var rows = h.collect(15, TimeUnit.SECONDS);
                assertThat(rows).hasSize(5);
            }
        }
    }

    @Test
    void innerJoin_withWhere_filtersRowsCorrectly() throws Exception {
        // Phase 4b.1: WHERE composes with shuffle-hash join. Combine SQL is
        // the original verbatim, so the WHERE just filters per row inside
        // each combine bucket. No planner changes were needed — this test
        // exists to prove the guard removal is safe and end-to-end correct.
        try (var c = new TestCluster(/*shuffleWidth*/ 2)) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT docs.id, events.action FROM docs " +
                    "JOIN events ON docs.id = events.doc_id " +
                    "WHERE events.action = 'view'")) {
                var rows = h.collect(15, TimeUnit.SECONDS);
                // us-1 has 3 events (view, download, share); eu-1 has 1 (download);
                // eu-2 has 1 (view). Only 2 rows have action='view'.
                assertThat(rows).hasSize(2);
                assertThat(rows).extracting(r -> r.get("id").asText())
                        .containsExactlyInAnyOrder("us-1", "eu-2");
            }
        }
    }

    @Test
    void innerJoin_withMultiSidePredicate_correct() throws Exception {
        // WHERE that touches both sides (docs.title-based filter + events
        // action-based filter). Verifies the predicate applies over the
        // joined row, not just one side.
        try (var c = new TestCluster(/*shuffleWidth*/ 3)) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT docs.id, docs.title, events.action FROM docs " +
                    "JOIN events ON docs.id = events.doc_id " +
                    "WHERE docs.title = 'Quarterly Report' AND events.action = 'download'")) {
                var rows = h.collect(15, TimeUnit.SECONDS);
                // Only us-1 has title='Quarterly Report' AND one of its events is 'download'
                assertThat(rows).hasSize(1);
                assertThat(rows.get(0).get("id").asText()).isEqualTo("us-1");
                assertThat(rows.get(0).get("action").asText()).isEqualTo("download");
            }
        }
    }

    @Test
    void leftOuterJoin_withWhere_filtersMatchedRows() throws Exception {
        // WHERE composes with LEFT OUTER: after the join expands unmatched
        // left rows with null-padded right, the WHERE filters the union.
        // Note: predicates on the null-padded side reject the unmatched
        // rows (null != any literal). Consistent with SQL semantics.
        try (var c = new TestCluster(/*shuffleWidth*/ 2)) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT docs.id, events.action FROM docs " +
                    "LEFT JOIN events ON docs.id = events.doc_id " +
                    "WHERE events.action = 'view'")) {
                var rows = h.collect(15, TimeUnit.SECONDS);
                // WHERE events.action='view' drops null-padded rows too.
                // Only us-1 (view) and eu-2 (view) survive.
                assertThat(rows).hasSize(2);
                assertThat(rows).extracting(r -> r.get("id").asText())
                        .containsExactlyInAnyOrder("us-1", "eu-2");
            }
        }
    }

    @Test
    void innerJoin_withGroupByCount_aggregatesAcrossBuckets() throws Exception {
        // Phase 4b.2: shuffle-hash join + GROUP BY + COUNT. Per-bucket
        // combines emit partial group counts; driver reduces them across
        // buckets. us-1 has 3 events, eu-1 has 1, eu-2 has 1, us-2 has 0.
        // GROUP BY docs.id: {us-1: 3, eu-1: 1, eu-2: 1}. us-2 doesn't appear
        // (INNER drops the unmatched left).
        try (var c = new TestCluster(/*shuffleWidth*/ 2)) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT docs.id, COUNT(*) FROM docs " +
                    "JOIN events ON docs.id = events.doc_id " +
                    "GROUP BY docs.id")) {
                var rows = h.collect(15, TimeUnit.SECONDS);
                assertThat(rows).hasSize(3);
                Map<String, Long> byId = new HashMap<>();
                for (JsonNode r : rows) byId.put(r.get("id").asText(), r.get("c0").asLong());
                assertThat(byId).containsExactlyInAnyOrderEntriesOf(Map.of(
                        "us-1", 3L, "eu-1", 1L, "eu-2", 1L));
            }
        }
    }

    @Test
    void innerJoin_groupByRightSideCol_aggregatesAcrossBuckets() throws Exception {
        // GROUP BY a column from the right side (events.action). The group
        // col comes from a table that isn't the "leading" FROM — exercises
        // the type-lookup fallback across left+right table types.
        try (var c = new TestCluster(/*shuffleWidth*/ 3)) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT events.action, COUNT(*) FROM docs " +
                    "JOIN events ON docs.id = events.doc_id " +
                    "GROUP BY events.action")) {
                var rows = h.collect(15, TimeUnit.SECONDS);
                // us-1: view, download, share; eu-1: download; eu-2: view
                // → download=2, view=2, share=1
                Map<String, Long> byAction = new HashMap<>();
                for (JsonNode r : rows) byAction.put(r.get("action").asText(), r.get("c0").asLong());
                assertThat(byAction).containsExactlyInAnyOrderEntriesOf(Map.of(
                        "download", 2L, "view", 2L, "share", 1L));
            }
        }
    }

    @Test
    void innerJoin_withGroupBy_wideShuffle_stillCorrect() throws Exception {
        // Same aggregate at a wider shuffle width — same expected result.
        // Verifies the driver's final combine correctly reduces partials
        // even when they're spread across many buckets.
        try (var c = new TestCluster(/*shuffleWidth*/ 4)) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT docs.id, COUNT(*) FROM docs " +
                    "JOIN events ON docs.id = events.doc_id " +
                    "GROUP BY docs.id")) {
                var rows = h.collect(15, TimeUnit.SECONDS);
                assertThat(rows).hasSize(3);
                Map<String, Long> byId = new HashMap<>();
                for (JsonNode r : rows) byId.put(r.get("id").asText(), r.get("c0").asLong());
                assertThat(byId).containsExactlyInAnyOrderEntriesOf(Map.of(
                        "us-1", 3L, "eu-1", 1L, "eu-2", 1L));
            }
        }
    }

    @Test
    void innerJoin_withGroupByAndWhere_filtersBeforeAggregate() throws Exception {
        // GROUP BY + WHERE composed. WHERE filters events, GROUP BY reduces
        // survivors. Only 'view' + 'download' actions survive → docs.id
        // counts are: us-1=2 (view, download), eu-1=1 (download), eu-2=1 (view).
        try (var c = new TestCluster(/*shuffleWidth*/ 2)) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT docs.id, COUNT(*) FROM docs " +
                    "JOIN events ON docs.id = events.doc_id " +
                    "WHERE events.action IN ('view', 'download') " +
                    "GROUP BY docs.id")) {
                var rows = h.collect(15, TimeUnit.SECONDS);
                Map<String, Long> byId = new HashMap<>();
                for (JsonNode r : rows) byId.put(r.get("id").asText(), r.get("c0").asLong());
                assertThat(byId).containsExactlyInAnyOrderEntriesOf(Map.of(
                        "us-1", 2L, "eu-1", 1L, "eu-2", 1L));
            }
        }
    }

    @Test
    void innerJoin_globalCount_noGroupBy_reducesToSingleRow() throws Exception {
        // Global aggregate over a JOIN — no GROUP BY, so the whole join
        // reduces to a single row. us-1: 3 events, eu-1: 1, eu-2: 1 → 5
        // matched rows total.
        try (var c = new TestCluster(/*shuffleWidth*/ 2)) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT COUNT(*) FROM docs " +
                    "JOIN events ON docs.id = events.doc_id")) {
                var rows = h.collect(15, TimeUnit.SECONDS);
                assertThat(rows).hasSize(1);
                assertThat(rows.get(0).get("c0").asLong()).isEqualTo(5L);
            }
        }
    }

    @Test
    void innerJoin_globalCount_widerShuffle_stillOneRow() throws Exception {
        // Even with more buckets, the driver's final combine reduces to
        // a single row.
        try (var c = new TestCluster(/*shuffleWidth*/ 4)) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT COUNT(*) FROM docs " +
                    "JOIN events ON docs.id = events.doc_id")) {
                var rows = h.collect(15, TimeUnit.SECONDS);
                assertThat(rows).hasSize(1);
                assertThat(rows.get(0).get("c0").asLong()).isEqualTo(5L);
            }
        }
    }

    @Test
    void innerJoin_skewedKeys_stillPairsCorrectly() throws Exception {
        // us-1 has 3 events, us-2 has 0, eu-1 has 1, eu-2 has 1.
        // A skewed hash distribution should still get every event on the
        // same bucket as its doc.
        try (var c = new TestCluster(/*shuffleWidth*/ 3)) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT docs.id, events.action FROM docs " +
                    "JOIN events ON docs.id = events.doc_id")) {
                var rows = h.collect(15, TimeUnit.SECONDS);
                Map<String, Long> byDoc = new HashMap<>();
                for (JsonNode r : rows) byDoc.merge(r.get("id").asText(), 1L, Long::sum);
                assertThat(byDoc).containsExactlyInAnyOrderEntriesOf(Map.of(
                        "us-1", 3L, "eu-1", 1L, "eu-2", 1L));
            }
        }
    }

    // -- cluster harness with docs + events tables ---------------------------

    private static final class TestCluster implements AutoCloseable {
        private final MeshTransport transport = new InMemoryMeshTransport();
        private final MeshDriver driver;
        private final List<MeshAgent> agents = new ArrayList<>();

        TestCluster(int shuffleWidth) throws Exception {
            DistributedTableRegistry tables = new DistributedTableRegistry();

            Type docsType = docsType();
            List<DistributedTable.Partition> docsParts = List.of(
                    new DistributedTable.Partition("us",
                            Set.of("jvssql", "partition:docs:us"), 2),
                    new DistributedTable.Partition("eu",
                            Set.of("jvssql", "partition:docs:eu"), 2));
            tables.register(new SimpleTable("docs", docsType, docsParts));

            Type eventsType = eventsType();
            List<DistributedTable.Partition> evtParts = List.of(
                    new DistributedTable.Partition("us",
                            Set.of("jvssql", "partition:events:us"), 3),
                    new DistributedTable.Partition("eu",
                            Set.of("jvssql", "partition:events:eu"), 2));
            tables.register(new SimpleTable("events", eventsType, evtParts));

            driver = new MeshDriver(transport, tables, 10_000);
            driver.dispatcher().withShuffleWidth(shuffleWidth);
            driver.start();

            // 2 docs-holding agents.
            agents.add(spawnAgent("agent-docs-us", "docs", docsType, "us", docsUs()));
            agents.add(spawnAgent("agent-docs-eu", "docs", docsType, "eu", docsEu()));
            // 2 events-holding agents.
            agents.add(spawnAgent("agent-events-us", "events", eventsType, "us", eventsUs()));
            agents.add(spawnAgent("agent-events-eu", "events", eventsType, "eu", eventsEu()));

            waitForAgents(4);
        }

        private MeshAgent spawnAgent(String id, String tableName, Type type, String pk, List<JVS> rows) {
            AgentConfig cfg = new AgentConfig(
                    id,
                    Set.of("jvssql", "partition:" + tableName + ":" + pk),
                    Duration.ofMillis(100),
                    List.of(new StaticTable(tableName, type, pk, rows)));
            MeshAgent a = new MeshAgent(transport, cfg);
            a.start();
            return a;
        }

        private void waitForAgents(int expected) throws InterruptedException {
            long deadline = System.currentTimeMillis() + 2_000;
            while (System.currentTimeMillis() < deadline) {
                if (driver.agents().liveCount() >= expected) return;
                Thread.sleep(25);
            }
            throw new IllegalStateException("only " + driver.agents().liveCount() + "/" + expected);
        }

        MeshDriver driver() { return driver; }

        @Override public void close() {
            for (var a : agents) a.close();
            driver.close();
            transport.close();
        }
    }

    // -- schemas + data ------------------------------------------------------

    private static Type docsType() throws Exception {
        Type t = new Type();
        t.init(MAPPER.readTree("{\"name\":\"docs\",\"fields\":["
                + "{\"name\":\"id\",\"type\":\"core_string\"},"
                + "{\"name\":\"title\",\"type\":\"core_string\"}]}"));
        return t;
    }

    private static Type eventsType() throws Exception {
        Type t = new Type();
        t.init(MAPPER.readTree("{\"name\":\"events\",\"fields\":["
                + "{\"name\":\"doc_id\",\"type\":\"core_string\"},"
                + "{\"name\":\"action\",\"type\":\"core_string\"}]}"));
        return t;
    }

    private static List<JVS> docsUs() throws Exception {
        return List.of(
            row("{\"id\":\"us-1\",\"title\":\"Quarterly Report\"}"),
            row("{\"id\":\"us-2\",\"title\":\"Roadmap Q4\"}"));
    }
    private static List<JVS> docsEu() throws Exception {
        return List.of(
            row("{\"id\":\"eu-1\",\"title\":\"Rapport Annuel\"}"),
            row("{\"id\":\"eu-2\",\"title\":\"Marktbericht\"}"));
    }
    private static List<JVS> eventsUs() throws Exception {
        return List.of(
            row("{\"doc_id\":\"us-1\",\"action\":\"view\"}"),
            row("{\"doc_id\":\"us-1\",\"action\":\"download\"}"),
            row("{\"doc_id\":\"us-1\",\"action\":\"share\"}"));
    }
    private static List<JVS> eventsEu() throws Exception {
        return List.of(
            row("{\"doc_id\":\"eu-1\",\"action\":\"download\"}"),
            row("{\"doc_id\":\"eu-2\",\"action\":\"view\"}"));
    }

    private static JVS row(String json) throws Exception {
        return new JVS(MAPPER.readTree(json));
    }

    private record SimpleTable(String name, Type type, List<DistributedTable.Partition> partitions)
            implements DistributedTable {}

    private static final class StaticTable implements LocalTable {
        private final String name; private final Type type;
        private final String partitionKey; private final List<JVS> rows;
        StaticTable(String name, Type type, String pk, List<JVS> rows) {
            this.name = name; this.type = type; this.partitionKey = pk; this.rows = rows;
        }
        @Override public String name() { return name; }
        @Override public Type type() { return type; }
        @Override public String partitionKey() { return partitionKey; }
        @Override public Iterator<JVS> openScan() { return new ArrayList<>(rows).iterator(); }
    }
}
