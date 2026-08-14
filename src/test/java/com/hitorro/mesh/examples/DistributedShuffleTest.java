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
import com.hitorro.mesh.driver.QueryDispatcher;
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
 * Phase 2.5.1: distributed combiner correctness.
 *
 * <p>Runs the exact same GROUP BY queries as {@link DistributedAggregateTest}
 * but with {@code shuffleWidth > 0} — the combine happens on stage-1 worker
 * tasks fed from shuffle bucket subjects, not on the driver. Assertions match
 * the phase-2 tests exactly: any correctness regression from the shuffle
 * implementation shows up here.</p>
 */
class DistributedShuffleTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void countByGroup_shuffleWidth2_correctGlobalCounts() throws Exception {
        try (var c = new TestCluster(new DocsByLanguageExample(), /*shuffleWidth*/ 2)) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT lang, COUNT(*) FROM docs GROUP BY lang")) {
                var rows = h.collect(15, TimeUnit.SECONDS);
                Map<String, Long> byLang = new HashMap<>();
                for (JsonNode r : rows) byLang.put(r.get("lang").asText(), r.get("c0").asLong());
                // Same expected result as the phase-2 combiner-at-driver test.
                assertThat(byLang).containsExactlyInAnyOrderEntriesOf(Map.of(
                        "en", 5L, "fr", 1L, "de", 1L, "ja", 1L, "ko", 1L));
            }
        }
    }

    @Test
    void sumByGroup_shuffleWidth3_matchesPartitionCount() throws Exception {
        try (var c = new TestCluster(new DocsByLanguageExample(), /*shuffleWidth*/ 3)) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT lang, SUM(size_kb) FROM docs WHERE lang = 'en' GROUP BY lang")) {
                var rows = h.collect(15, TimeUnit.SECONDS);
                assertThat(rows).hasSize(1);
                assertThat(rows.get(0).get("c0").asLong()).isEqualTo(1299L);
            }
        }
    }

    @Test
    void minMaxByGroup_shuffleWidth3_correct() throws Exception {
        try (var c = new TestCluster(new DocsByLanguageExample(), /*shuffleWidth*/ 3)) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT lang, MIN(size_kb), MAX(size_kb) FROM docs GROUP BY lang")) {
                var rows = h.collect(15, TimeUnit.SECONDS);
                Map<String, long[]> byLang = new HashMap<>();
                for (JsonNode r : rows) {
                    byLang.put(r.get("lang").asText(),
                            new long[]{ r.get("c0").asLong(), r.get("c1").asLong() });
                }
                assertThat(byLang.get("en")).containsExactly(45L, 640L);
                assertThat(byLang.get("fr")).containsExactly(512L, 512L);
            }
        }
    }

    @Test
    void countPlusSum_shuffleWidth2_bothAggregatesCombine() throws Exception {
        try (var c = new TestCluster(new DocsByLanguageExample(), /*shuffleWidth*/ 2)) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT lang, COUNT(*), SUM(size_kb) FROM docs WHERE lang = 'en' GROUP BY lang")) {
                var rows = h.collect(15, TimeUnit.SECONDS);
                assertThat(rows).hasSize(1);
                var r = rows.get(0);
                assertThat(r.get("lang").asText()).isEqualTo("en");
                assertThat(r.get("c0").asLong()).isEqualTo(5L);
                assertThat(r.get("c1").asLong()).isEqualTo(1299L);
            }
        }
    }

    /**
     * Sanity check: shuffleWidth=0 still uses the phase-2 combiner-at-driver
     * path. Deterministically reproduces the same result as the phase-2 test.
     */
    @Test
    void shuffleWidthZero_fallsBackToCombinerAtDriver() throws Exception {
        try (var c = new TestCluster(new DocsByLanguageExample(), /*shuffleWidth*/ 0)) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT lang, COUNT(*) FROM docs GROUP BY lang")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                assertThat(rows).hasSize(5);
            }
        }
    }

    // -- cluster harness (parameterized with shuffleWidth) --------------------

    private static final class TestCluster implements AutoCloseable {
        private final MeshTransport transport = new InMemoryMeshTransport();
        private final MeshDriver driver;
        private final List<MeshAgent> agents = new ArrayList<>();

        TestCluster(HitorroMeshExample ex, int shuffleWidth) throws Exception {
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
            driver.dispatcher().withShuffleWidth(shuffleWidth);
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
            throw new IllegalStateException("only " + driver.agents().liveCount() + "/" + expected + " agents live");
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
        private final String name;
        private final Type type;
        private final String partitionKey;
        private final List<JVS> rows;
        StaticTable(String name, Type type, String pk, List<JVS> rows) {
            this.name = name; this.type = type; this.partitionKey = pk; this.rows = rows;
        }
        @Override public String name() { return name; }
        @Override public Type type() { return type; }
        @Override public String partitionKey() { return partitionKey; }
        @Override public Iterator<JVS> openScan() { return new ArrayList<>(rows).iterator(); }
    }
}
