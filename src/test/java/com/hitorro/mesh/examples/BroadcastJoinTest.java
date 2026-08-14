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
 * Phase 4a: broadcast JOIN.
 *
 * <p>Cluster shape: 3 agents each holding one partition of {@code docs}
 * (us / eu / apac) plus a shared broadcast table {@code langs} that maps
 * ISO language codes to display names. Every agent has the full langs
 * table locally (pre-loaded via {@link AgentConfig#broadcastTables()}).
 * Joins execute per-partition, results flow through the existing
 * combine/union machinery.</p>
 */
class BroadcastJoinTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void joinToBroadcastTable_enrichesRowsAcrossPartitions() throws Exception {
        try (var c = new TestCluster(3)) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT docs.id, docs.lang, langs.name FROM docs " +
                    "JOIN langs ON docs.lang = langs.code")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                // All 9 docs get their lang name attached.
                assertThat(rows).hasSize(9);
                // Spot-check a couple of the joins.
                Map<String, String> idToName = new HashMap<>();
                for (JsonNode r : rows) idToName.put(r.get("id").asText(), r.get("name").asText());
                assertThat(idToName).containsEntry("us-1", "English");
                assertThat(idToName).containsEntry("eu-1", "French");
                assertThat(idToName).containsEntry("ap-1", "Japanese");
            }
        }
    }

    @Test
    void joinWithWhere_filtersBeforeJoin() throws Exception {
        try (var c = new TestCluster(3)) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT docs.id, langs.name FROM docs " +
                    "JOIN langs ON docs.lang = langs.code " +
                    "WHERE docs.size_kb > 200")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                // size_kb > 200: us-1(210), us-3(640), eu-1(512), ap-1(190→no), ap-3(320)
                // Wait — ap-1 has 190, which is NOT > 200. So we should get us-1, us-3, eu-1, ap-3.
                assertThat(rows).hasSize(4);
                List<String> ids = rows.stream().map(r -> r.get("id").asText()).sorted().toList();
                assertThat(ids).containsExactly("ap-3", "eu-1", "us-1", "us-3");
            }
        }
    }

    @Test
    void joinWithGroupBy_aggregatesEnrichedRows() throws Exception {
        try (var c = new TestCluster(3)) {
            // Group by the broadcast column: count docs per language name.
            try (var h = c.driver().dispatcher().submit(
                    "SELECT langs.name, COUNT(*) FROM docs " +
                    "JOIN langs ON docs.lang = langs.code " +
                    "GROUP BY langs.name")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                Map<String, Long> byName = new HashMap<>();
                for (JsonNode r : rows) byName.put(r.get("name").asText(), r.get("c0").asLong());
                assertThat(byName).containsExactlyInAnyOrderEntriesOf(Map.of(
                        "English", 5L, "French", 1L, "German", 1L,
                        "Japanese", 1L, "Korean", 1L));
            }
        }
    }

    @Test
    void joinWithGroupBy_shuffleWidth2_stillCorrect() throws Exception {
        try (var c = new TestCluster(3, /*shuffleWidth*/ 2)) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT langs.name, COUNT(*) FROM docs " +
                    "JOIN langs ON docs.lang = langs.code " +
                    "GROUP BY langs.name")) {
                var rows = h.collect(10, TimeUnit.SECONDS);
                Map<String, Long> byName = new HashMap<>();
                for (JsonNode r : rows) byName.put(r.get("name").asText(), r.get("c0").asLong());
                assertThat(byName).containsExactlyInAnyOrderEntriesOf(Map.of(
                        "English", 5L, "French", 1L, "German", 1L,
                        "Japanese", 1L, "Korean", 1L));
            }
        }
    }

    // -- cluster harness with broadcast tables --------------------------------

    private static final class TestCluster implements AutoCloseable {
        private final MeshTransport transport = new InMemoryMeshTransport();
        private final MeshDriver driver;
        private final List<MeshAgent> agents = new ArrayList<>();

        TestCluster(int expectedAgents) throws Exception {
            this(expectedAgents, 0);
        }

        TestCluster(int expectedAgents, int shuffleWidth) throws Exception {
            DocsByLanguageExample ex = new DocsByLanguageExample();
            DistributedTableRegistry tables = new DistributedTableRegistry();
            List<DistributedTable.Partition> parts = new ArrayList<>();
            for (var pd : ex.partitions()) {
                parts.add(new DistributedTable.Partition(
                        pd.key(),
                        Set.of("jvssql", "partition:" + ex.tableName() + ":" + pd.key()),
                        pd.rows().size()));
            }
            tables.register(new SimpleTable(ex.tableName(), ex.tableType(), parts));
            tables.registerBroadcast("langs");
            driver = new MeshDriver(transport, tables, 10_000);
            driver.dispatcher().withShuffleWidth(shuffleWidth);
            driver.start();

            Type langsType = langsType();
            List<JVS> langsRows = langsRows();

            for (var pd : ex.partitions()) {
                LocalTable partition = new StaticTable(ex.tableName(), ex.tableType(), pd.key(), pd.rows());
                LocalTable broadcast = new StaticTable("langs", langsType, /*pk*/ null, langsRows);
                AgentConfig cfg = new AgentConfig(
                        "agent-" + pd.key(),
                        Set.of("jvssql", "partition:" + ex.tableName() + ":" + pd.key()),
                        Duration.ofMillis(100),
                        List.of(partition),
                        List.of(broadcast));
                MeshAgent a = new MeshAgent(transport, cfg);
                a.start();
                agents.add(a);
            }
            waitForAgents(expectedAgents);
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

    // -- broadcast table content ---------------------------------------------

    private static Type langsType() {
        try {
            Type t = new Type();
            t.init(MAPPER.readTree("{\"name\":\"langs\",\"fields\":["
                    + "{\"name\":\"code\", \"type\":\"core_string\"},"
                    + "{\"name\":\"name\", \"type\":\"core_string\"}"
                    + "]}"));
            return t;
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    private static List<JVS> langsRows() {
        try {
            return List.of(
                jvsRow("{\"code\":\"en\",\"name\":\"English\"}"),
                jvsRow("{\"code\":\"fr\",\"name\":\"French\"}"),
                jvsRow("{\"code\":\"de\",\"name\":\"German\"}"),
                jvsRow("{\"code\":\"ja\",\"name\":\"Japanese\"}"),
                jvsRow("{\"code\":\"ko\",\"name\":\"Korean\"}"));
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    private static JVS jvsRow(String json) throws Exception {
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
