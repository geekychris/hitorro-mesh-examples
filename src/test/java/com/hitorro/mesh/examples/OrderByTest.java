/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.examples;

import com.fasterxml.jackson.databind.JsonNode;
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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Phase 5b: distributed ORDER BY. Agents sort locally via jvssql; driver does
 * a global re-sort over the collected rows.
 */
class OrderByTest {

    @Test
    void orderBy_singleColumn_ascending() throws Exception {
        try (var c = new TestCluster()) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT id, size_kb FROM docs ORDER BY size_kb")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                assertThat(rows).hasSize(9);
                long prev = Long.MIN_VALUE;
                for (JsonNode r : rows) {
                    long v = r.get("size_kb").asLong();
                    assertThat(v).isGreaterThanOrEqualTo(prev);
                    prev = v;
                }
            }
        }
    }

    @Test
    void orderBy_singleColumn_descending() throws Exception {
        try (var c = new TestCluster()) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT id, size_kb FROM docs ORDER BY size_kb DESC")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                assertThat(rows).hasSize(9);
                assertThat(rows.get(0).get("id").asText()).isEqualTo("us-3");  // 640, largest
                assertThat(rows.get(rows.size() - 1).get("id").asText()).isEqualTo("eu-3");  // 45, smallest
            }
        }
    }

    @Test
    void orderBy_withWhere_filtersBeforeSorting() throws Exception {
        try (var c = new TestCluster()) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT id, size_kb FROM docs WHERE lang = 'en' ORDER BY size_kb DESC")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                assertThat(rows).hasSize(5);
                // en size_kb DESC: us-3 (640), ap-3 (320), us-1 (210), us-2 (84), eu-3 (45)
                assertThat(rows.get(0).get("id").asText()).isEqualTo("us-3");
                assertThat(rows.get(1).get("id").asText()).isEqualTo("ap-3");
                assertThat(rows.get(2).get("id").asText()).isEqualTo("us-1");
            }
        }
    }

    @Test
    void orderBy_withLimit_topN() throws Exception {
        try (var c = new TestCluster()) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT id, size_kb FROM docs ORDER BY size_kb DESC LIMIT 3")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                assertThat(rows).hasSize(3);
                // Top-3 largest: us-3 (640), eu-1 (512), ap-3 (320)
                assertThat(rows.get(0).get("id").asText()).isEqualTo("us-3");
                assertThat(rows.get(1).get("id").asText()).isEqualTo("eu-1");
                assertThat(rows.get(2).get("id").asText()).isEqualTo("ap-3");
            }
        }
    }

    @Test
    void orderBy_multiColumn_leftDominates() throws Exception {
        try (var c = new TestCluster()) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT id, lang, size_kb FROM docs ORDER BY lang ASC, size_kb DESC")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                assertThat(rows).hasSize(9);
                // First rows are lang=de (alphabetically first).
                assertThat(rows.get(0).get("lang").asText()).isEqualTo("de");
                // Last rows are lang=ko (alphabetically last among en/fr/de/ja/ko).
                assertThat(rows.get(rows.size() - 1).get("lang").asText()).isEqualTo("ko");
                // Within lang=en (rows 1..5), size_kb should be descending: 640, 320, 210, 84, 45.
                List<Long> enSizes = new ArrayList<>();
                for (JsonNode r : rows) {
                    if ("en".equals(r.get("lang").asText())) enSizes.add(r.get("size_kb").asLong());
                }
                assertThat(enSizes).containsExactly(640L, 320L, 210L, 84L, 45L);
            }
        }
    }

    @Test
    void orderBy_onStringColumn_lexicographic() throws Exception {
        try (var c = new TestCluster()) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT id FROM docs ORDER BY id")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                List<String> ids = rows.stream().map(r -> r.get("id").asText()).toList();
                // ap-1, ap-2, ap-3, eu-1, eu-2, eu-3, us-1, us-2, us-3
                assertThat(ids).containsExactly(
                        "ap-1", "ap-2", "ap-3", "eu-1", "eu-2", "eu-3", "us-1", "us-2", "us-3");
            }
        }
    }

    @Test
    void orderBy_withGroupBy_composesCorrectly() throws Exception {
        try (var c = new TestCluster()) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT lang, COUNT(*) FROM docs GROUP BY lang ORDER BY lang")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                assertThat(rows).hasSize(5);
                List<String> langs = rows.stream().map(r -> r.get("lang").asText()).toList();
                // Alphabetical: de, en, fr, ja, ko
                assertThat(langs).containsExactly("de", "en", "fr", "ja", "ko");
            }
        }
    }

    @Test
    void orderBy_onAggregate_topByCount() throws Exception {
        // Phase 5b.1: ORDER BY the aggregate output column.
        try (var c = new TestCluster()) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT lang, COUNT(*) FROM docs GROUP BY lang ORDER BY c0 DESC LIMIT 3")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                assertThat(rows).hasSize(3);
                // Top-3 by count: en=5, then any three of {fr, de, ja, ko}=1
                assertThat(rows.get(0).get("lang").asText()).isEqualTo("en");
                assertThat(rows.get(0).get("c0").asLong()).isEqualTo(5L);
                assertThat(rows.get(1).get("c0").asLong()).isEqualTo(1L);
                assertThat(rows.get(2).get("c0").asLong()).isEqualTo(1L);
            }
        }
    }

    // -- cluster harness ------------------------------------------------------

    private static final class TestCluster implements AutoCloseable {
        private final MeshTransport transport = new InMemoryMeshTransport();
        private final MeshDriver driver;
        private final List<MeshAgent> agents = new ArrayList<>();

        TestCluster() throws Exception {
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
            driver = new MeshDriver(transport, tables, 10_000);
            driver.start();
            for (var pd : ex.partitions()) {
                AgentConfig cfg = new AgentConfig(
                        "agent-" + pd.key(),
                        Set.of("jvssql", "partition:" + ex.tableName() + ":" + pd.key()),
                        Duration.ofMillis(100),
                        List.of(new StaticTable(ex.tableName(), ex.tableType(), pd.key(), pd.rows())));
                MeshAgent a = new MeshAgent(transport, cfg);
                a.start();
                agents.add(a);
            }
            waitForAgents(ex.partitions().size());
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
