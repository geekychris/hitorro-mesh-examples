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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 3: AVG, SELECT DISTINCT, HAVING.
 *
 * <p>Each exercise runs on the combiner-at-driver path (shuffle-width=0).
 * The shuffle path is exercised by {@link DistributedShuffleTest} for the
 * phase-2 aggregate set; adding shuffle coverage for the phase-3 shapes
 * is a follow-up.</p>
 */
class Phase3AggregatesTest {

    // -- AVG ------------------------------------------------------------------

    @Test
    void avg_singleGroup_correctMean() throws Exception {
        try (var c = new TestCluster(new DocsByLanguageExample())) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT lang, AVG(size_kb) FROM docs WHERE lang = 'en' GROUP BY lang")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                assertThat(rows).hasSize(1);
                // en size_kb values: 210, 84, 640, 45, 320 → sum=1299, count=5, avg=259.8
                assertThat(rows.get(0).get("c0").asDouble()).isEqualTo(259.8);
            }
        }
    }

    @Test
    void avg_multipleGroups_combinesCorrectly() throws Exception {
        try (var c = new TestCluster(new DocsByLanguageExample())) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT lang, AVG(size_kb) FROM docs GROUP BY lang")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                Map<String, Double> avgs = new HashMap<>();
                for (JsonNode r : rows) avgs.put(r.get("lang").asText(), r.get("c0").asDouble());
                assertThat(avgs).containsKeys("en", "fr", "de", "ja", "ko");
                // fr: single value 512.0
                assertThat(avgs.get("fr")).isEqualTo(512.0);
                // en: (210+84+640+45+320)/5 = 259.8
                assertThat(avgs.get("en")).isEqualTo(259.8);
            }
        }
    }

    @Test
    void avg_alongsideCount_bothWork() throws Exception {
        try (var c = new TestCluster(new DocsByLanguageExample())) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT lang, COUNT(*), AVG(size_kb) FROM docs WHERE lang = 'en' GROUP BY lang")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                assertThat(rows).hasSize(1);
                assertThat(rows.get(0).get("c0").asLong()).isEqualTo(5L);
                assertThat(rows.get(0).get("c1").asDouble()).isEqualTo(259.8);
            }
        }
    }

    // -- SELECT DISTINCT ------------------------------------------------------

    @Test
    void selectDistinct_deduplicatesAcrossPartitions() throws Exception {
        try (var c = new TestCluster(new DocsByLanguageExample())) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT DISTINCT lang FROM docs")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                List<String> langs = new ArrayList<>();
                for (JsonNode r : rows) langs.add(r.get("lang").asText());
                assertThat(langs).containsExactlyInAnyOrder("en", "fr", "de", "ja", "ko");
                // 9 documents but only 5 distinct langs — dedup worked
                assertThat(langs).hasSize(5);
            }
        }
    }

    @Test
    void selectDistinct_withWhere_appliesFilterBeforeDedup() throws Exception {
        try (var c = new TestCluster(new DocsByLanguageExample())) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT DISTINCT lang FROM docs WHERE size_kb > 100")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                List<String> langs = new ArrayList<>();
                for (JsonNode r : rows) langs.add(r.get("lang").asText());
                // size_kb > 100: us-1(en,210), us-3(en,640), eu-1(fr,512), eu-2(de,128),
                //                ap-1(ja,190), ap-3(en,320) → distinct langs: en, fr, de, ja
                assertThat(langs).containsExactlyInAnyOrder("en", "fr", "de", "ja");
            }
        }
    }

    // -- HAVING ---------------------------------------------------------------

    @Test
    void having_filtersAggregatedGroups() throws Exception {
        try (var c = new TestCluster(new DocsByLanguageExample())) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT lang, COUNT(*) FROM docs GROUP BY lang HAVING COUNT(*) > 1")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                Map<String, Long> byLang = new HashMap<>();
                for (JsonNode r : rows) byLang.put(r.get("lang").asText(), r.get("c0").asLong());
                // Only "en" has more than one row (5); everyone else has exactly 1.
                assertThat(byLang).containsExactlyInAnyOrderEntriesOf(Map.of("en", 5L));
            }
        }
    }

    @Test
    void having_onSum_filtersCorrectly() throws Exception {
        try (var c = new TestCluster(new DocsByLanguageExample())) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT lang, SUM(size_kb) FROM docs GROUP BY lang HAVING SUM(size_kb) > 300")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                Map<String, Long> sums = new HashMap<>();
                for (JsonNode r : rows) sums.put(r.get("lang").asText(), r.get("c0").asLong());
                // en=1299, fr=512, de=128, ja=190, ko=55  → groups with sum>300: en, fr
                assertThat(sums).containsExactlyInAnyOrderEntriesOf(Map.of(
                        "en", 1299L, "fr", 512L));
            }
        }
    }

    // -- cluster harness ------------------------------------------------------

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
