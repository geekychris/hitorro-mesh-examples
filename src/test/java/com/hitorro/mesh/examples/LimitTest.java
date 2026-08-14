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

/**
 * Phase 5a: driver-side LIMIT.
 *
 * <p>Independent of ORDER BY (phase 5b) — no sort guarantees, just a hard
 * cap on rows returned to the client. Applied to every plan shape (simple,
 * two-stage, shuffle-join).</p>
 */
class LimitTest {

    @Test
    void limitCapsRowsFromSimpleQuery() throws Exception {
        try (var c = new TestCluster()) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT id, title FROM docs LIMIT 3")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                // Dataset has 9 docs total; LIMIT caps at 3.
                assertThat(rows).hasSize(3);
            }
        }
    }

    @Test
    void limitCapsRowsWithWhere() throws Exception {
        try (var c = new TestCluster()) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT id FROM docs WHERE lang = 'en' LIMIT 2")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                // 5 English docs available; LIMIT caps at 2.
                assertThat(rows).hasSize(2);
            }
        }
    }

    @Test
    void limitLargerThanTotal_returnsAllRows() throws Exception {
        try (var c = new TestCluster()) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT id FROM docs WHERE lang = 'fr' LIMIT 100")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                // Only 1 French doc; LIMIT 100 doesn't add rows.
                assertThat(rows).hasSize(1);
            }
        }
    }

    @Test
    void limitZero_returnsNoRows() throws Exception {
        try (var c = new TestCluster()) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT id FROM docs LIMIT 0")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                assertThat(rows).isEmpty();
            }
        }
    }

    @Test
    void limitAfterGroupBy_capsAggregatedRows() throws Exception {
        try (var c = new TestCluster()) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT lang, COUNT(*) FROM docs GROUP BY lang LIMIT 2")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                // 5 distinct langs; LIMIT caps at 2 aggregate rows.
                assertThat(rows).hasSize(2);
                // Each returned row should still be a proper aggregate.
                for (JsonNode r : rows) {
                    assertThat(r.get("lang").isTextual()).isTrue();
                    assertThat(r.get("c0").asLong()).isGreaterThan(0);
                }
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
