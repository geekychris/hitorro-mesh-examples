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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Phase-2 end-to-end tests: {@code GROUP BY} + associative aggregates
 * (COUNT/SUM/MIN/MAX) return the correct <b>globally aggregated</b> values,
 * not per-partition partials.
 */
class DistributedAggregateTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void countByGroup_combinesAcrossPartitions() throws Exception {
        try (var c = new TestCluster(new DocsByLanguageExample())) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT lang, COUNT(*) FROM docs GROUP BY lang")) {
                var rows = h.collect(5, TimeUnit.SECONDS);

                // From the DocsByLanguageExample dataset:
                //   en × 5 (us-1, us-2, us-3, eu-3, ap-3)
                //   fr × 1 (eu-1)
                //   de × 1 (eu-2)
                //   ja × 1 (ap-1)
                //   ko × 1 (ap-2)
                Map<String, Long> byLang = new HashMap<>();
                for (JsonNode r : rows) {
                    byLang.put(r.get("lang").asText(), r.get("c0").asLong());
                }
                assertThat(byLang).containsExactlyInAnyOrderEntriesOf(Map.of(
                        "en", 5L, "fr", 1L, "de", 1L, "ja", 1L, "ko", 1L));
            }
        }
    }

    @Test
    void sumByGroup_addsAcrossPartitions() throws Exception {
        try (var c = new TestCluster(new DocsByLanguageExample())) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT lang, SUM(size_kb) FROM docs WHERE lang = 'en' GROUP BY lang")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                assertThat(rows).hasSize(1);
                // en size_kb: 210 + 84 + 640 + 45 + 320 = 1299
                assertThat(rows.get(0).get("c0").asLong()).isEqualTo(1299L);
            }
        }
    }

    @Test
    void minMaxByGroup_reduceCorrectly() throws Exception {
        try (var c = new TestCluster(new DocsByLanguageExample())) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT lang, MIN(size_kb), MAX(size_kb) FROM docs GROUP BY lang")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                Map<String, long[]> byLang = new HashMap<>();
                for (JsonNode r : rows) {
                    byLang.put(r.get("lang").asText(),
                            new long[]{ r.get("c0").asLong(), r.get("c1").asLong() });
                }
                // en: min(45, 84, 210, 320, 640) = 45,  max = 640
                assertThat(byLang.get("en")).containsExactly(45L, 640L);
                // fr: single value
                assertThat(byLang.get("fr")).containsExactly(512L, 512L);
            }
        }
    }

    @Test
    void countPlusSum_bothAggregatesCombined() throws Exception {
        try (var c = new TestCluster(new DocsByLanguageExample())) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT lang, COUNT(*), SUM(size_kb) FROM docs WHERE lang = 'en' GROUP BY lang")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                assertThat(rows).hasSize(1);
                var r = rows.get(0);
                assertThat(r.get("lang").asText()).isEqualTo("en");
                assertThat(r.get("c0").asLong()).isEqualTo(5L);       // COUNT(*)
                assertThat(r.get("c1").asLong()).isEqualTo(1299L);    // SUM(size_kb)
            }
        }
    }

    @Test
    void groupByCarriesWhere_correctlyFiltersBeforeAggregating() throws Exception {
        try (var c = new TestCluster(new DocsByLanguageExample())) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT lang, COUNT(*) FROM docs WHERE size_kb > 100 GROUP BY lang")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                Map<String, Long> byLang = new HashMap<>();
                for (JsonNode r : rows) byLang.put(r.get("lang").asText(), r.get("c0").asLong());
                // size_kb > 100: us-1(210), us-3(640), eu-1(512), eu-2(128), ap-1(190), ap-3(320)
                //   en → 3 (us-1, us-3, ap-3), fr → 1 (eu-1), de → 1 (eu-2), ja → 1 (ap-1)
                assertThat(byLang).containsExactlyInAnyOrderEntriesOf(Map.of(
                        "en", 3L, "fr", 1L, "de", 1L, "ja", 1L));
            }
        }
    }

    @Test
    void aggregateWithoutGroupBy_stillRejectedForNow() throws Exception {
        try (var c = new TestCluster(new DocsByLanguageExample())) {
            assertThatThrownBy(() -> c.driver().dispatcher().submit(
                    "SELECT COUNT(*) FROM docs"))
                .hasMessageContaining("aggregate without GROUP BY not yet supported");
        }
    }

    /**
     * Phase 4a allows JOIN against registered broadcast tables. JOIN against
     * an unregistered / non-broadcast table is still rejected — that would
     * need shuffle-hash join (phase 4b). No broadcast tables registered on
     * this test cluster, so any JOIN fails.
     */
    @Test
    void joinToNonBroadcastTable_isRejected() throws Exception {
        try (var c = new TestCluster(new DocsByLanguageExample())) {
            assertThatThrownBy(() -> c.driver().dispatcher().submit(
                    "SELECT * FROM docs JOIN other ON docs.id = other.id"))
                .hasMessageContaining("only registered broadcast tables can be joined");
        }
    }

    // -- cluster harness (same shape as EndToEndTest) -------------------------

    private static final class TestCluster implements AutoCloseable {
        private final MeshTransport transport = new InMemoryMeshTransport();
        private final MeshDriver driver;
        private final List<MeshAgent> agents = new ArrayList<>();

        TestCluster(HitorroMeshExample ex) throws Exception {
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
