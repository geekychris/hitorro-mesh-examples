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
 * Phase 4c.1: shuffle-hash OUTER JOIN between two distributed tables.
 *
 * <p>Data shape: {@code docs(id, title)} and {@code events(doc_id, action)}.
 * Some docs have no events (docs.id ∉ events.doc_id) — LEFT preserves them.
 * Some events reference no doc (events.doc_id ∉ docs.id) — RIGHT preserves
 * them. FULL preserves both sides.</p>
 *
 * <p>Correctness hinges on:
 * <ol>
 *   <li>The planner detecting LEFT/RIGHT/FULL OUTER JOIN and setting
 *       {@code plan.joinKind()} accordingly.</li>
 *   <li>The dispatcher shipping each side's schema JSON in the CombineSpec
 *       so empty buckets have a typed stream to register.</li>
 *   <li>The combine worker's kind-aware short-circuit — LEFT does NOT
 *       collapse a bucket where the right side is empty (unlike INNER).</li>
 * </ol>
 * If any of those is broken, unmatched rows disappear.</p>
 */
class ShuffleOuterJoinTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void leftOuterJoin_preservesUnmatchedLeftRows() throws Exception {
        try (var c = new TestCluster(/*shuffleWidth*/ 2)) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT docs.id, docs.title, events.action FROM docs " +
                    "LEFT JOIN events ON docs.id = events.doc_id")) {
                var rows = h.collect(15, TimeUnit.SECONDS);
                // 4 docs. us-1 has 3 events → 3 rows. us-2 has none → 1 row (null).
                // eu-1 has 2 events → 2 rows. eu-2 has none → 1 row (null). Total = 7.
                assertThat(rows).hasSize(7);
                Map<String, List<String>> actionsById = groupActionsById(rows);
                assertThat(actionsById).containsOnlyKeys("us-1", "us-2", "eu-1", "eu-2");
                assertThat(actionsById.get("us-1")).hasSize(3);
                assertThat(actionsById.get("eu-1")).hasSize(2);
                assertThat(actionsById.get("us-2")).containsExactly((String) null);
                assertThat(actionsById.get("eu-2")).containsExactly((String) null);
            }
        }
    }

    @Test
    void leftOuterJoin_wideShuffle_stillCorrect() throws Exception {
        // With a wider shuffle grid the odds of some bucket getting only
        // left rows or only right rows go up — this exercises the empty-side
        // schema path more aggressively.
        try (var c = new TestCluster(/*shuffleWidth*/ 4)) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT docs.id, events.action FROM docs " +
                    "LEFT JOIN events ON docs.id = events.doc_id")) {
                var rows = h.collect(15, TimeUnit.SECONDS);
                assertThat(rows).hasSize(7);
            }
        }
    }

    @Test
    void rightOuterJoin_preservesUnmatchedRightRows() throws Exception {
        try (var c = new TestCluster(/*shuffleWidth*/ 2)) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT docs.id, events.action FROM docs " +
                    "RIGHT JOIN events ON docs.id = events.doc_id")) {
                var rows = h.collect(15, TimeUnit.SECONDS);
                // 6 events. 5 match a doc, 1 (doc_id=orphan) doesn't. Total = 6.
                assertThat(rows).hasSize(6);
                long orphanCount = rows.stream()
                        .filter(r -> {
                            JsonNode idNode = r.get("id");
                            return idNode == null || idNode.isNull();
                        })
                        .count();
                assertThat(orphanCount).as("one event references an orphan doc_id").isEqualTo(1);
            }
        }
    }

    @Test
    void fullOuterJoin_preservesBothUnmatchedSides() throws Exception {
        try (var c = new TestCluster(/*shuffleWidth*/ 2)) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT docs.id, events.action FROM docs " +
                    "FULL JOIN events ON docs.id = events.doc_id")) {
                var rows = h.collect(15, TimeUnit.SECONDS);
                // Matched: 5. Left-only docs: 2 (us-2, eu-2). Right-only event: 1 (orphan).
                // Total = 8.
                assertThat(rows).hasSize(8);
                long docOnly = rows.stream()
                        .filter(r -> {
                            JsonNode id = r.get("id");
                            JsonNode action = r.get("action");
                            return id != null && !id.isNull() && (action == null || action.isNull());
                        }).count();
                long eventOnly = rows.stream()
                        .filter(r -> {
                            JsonNode id = r.get("id");
                            return id == null || id.isNull();
                        }).count();
                assertThat(docOnly).as("2 docs with no matching events").isEqualTo(2);
                assertThat(eventOnly).as("1 orphan event").isEqualTo(1);
            }
        }
    }

    // -- helpers -------------------------------------------------------------

    private static Map<String, List<String>> groupActionsById(List<JsonNode> rows) {
        Map<String, List<String>> out = new HashMap<>();
        for (JsonNode r : rows) {
            String id = r.get("id").asText();
            JsonNode actionNode = r.get("action");
            String action = (actionNode == null || actionNode.isNull()) ? null : actionNode.asText();
            out.computeIfAbsent(id, k -> new ArrayList<>()).add(action);
        }
        return out;
    }

    // -- cluster harness -----------------------------------------------------

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
                            Set.of("jvssql", "partition:events:eu"), 3));
            tables.register(new SimpleTable("events", eventsType, evtParts));

            driver = new MeshDriver(transport, tables, 10_000);
            driver.dispatcher().withShuffleWidth(shuffleWidth);
            driver.start();

            agents.add(spawnAgent("agent-docs-us", "docs", docsType, "us", docsUs()));
            agents.add(spawnAgent("agent-docs-eu", "docs", docsType, "eu", docsEu()));
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

    // 4 docs total. us-2 and eu-2 have NO events → unmatched on LEFT/FULL.
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

    // 6 events total. Last one (doc_id=orphan) has NO matching doc →
    // unmatched on RIGHT/FULL.
    private static List<JVS> eventsUs() throws Exception {
        return List.of(
            row("{\"doc_id\":\"us-1\",\"action\":\"view\"}"),
            row("{\"doc_id\":\"us-1\",\"action\":\"download\"}"),
            row("{\"doc_id\":\"us-1\",\"action\":\"share\"}"));
    }
    private static List<JVS> eventsEu() throws Exception {
        return List.of(
            row("{\"doc_id\":\"eu-1\",\"action\":\"download\"}"),
            row("{\"doc_id\":\"eu-1\",\"action\":\"view\"}"),
            row("{\"doc_id\":\"orphan\",\"action\":\"scan\"}"));
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
