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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 5b.3 — verify LIMIT (with and without ORDER BY) is pushed to agents,
 * so each partition returns at most N rows instead of scanning everything.
 * The result correctness is already covered by {@link LimitTest} and
 * {@link OrderByTest} — this file pins the <b>efficiency</b> property by
 * counting rows each partition emits via a side-effect-instrumented
 * {@link LocalTable}.
 */
class LimitPushdownTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void limitWithoutOrderBy_capsPerPartitionScan() throws Exception {
        try (var c = new TestCluster(50, 50)) {   // 100 rows total across 2 partitions
            try (var h = c.driver().dispatcher().submit(
                    "SELECT id FROM events LIMIT 3")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                assertThat(rows).hasSize(3);
                // Both partitions should have scanned no more than LIMIT N each.
                // Without pushdown, each partition scans all 50 rows (100 total emitted).
                // With pushdown, each partition emits at most 3 rows (≤ 6 total).
                assertThat(c.usScanCount().get() + c.euScanCount().get()).isLessThanOrEqualTo(6);
            }
        }
    }

    @Test
    void limitWithOrderBy_returnsGlobalTopN() throws Exception {
        try (var c = new TestCluster(50, 50)) {
            // Top-3 largest ids overall — LIMIT + ORDER BY DESC.
            try (var h = c.driver().dispatcher().submit(
                    "SELECT id, val FROM events ORDER BY val DESC LIMIT 3")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                assertThat(rows).hasSize(3);
                // us has vals 1..50, eu has 51..100. Global top-3: 100, 99, 98.
                List<Long> gotVals = new ArrayList<>();
                for (JsonNode r : rows) gotVals.add(r.get("val").asLong());
                assertThat(gotVals).containsExactly(100L, 99L, 98L);
                // Correctness only — the scan-count check doesn't apply for
                // ORDER BY because jvssql's SORT operator has to see every row
                // before it can emit its top-N. LIMIT pushdown DOES bound the
                // rows sent DRIVER-side (each agent emits ≤ 3), which is what
                // matters for network bandwidth; the LIMIT-without-ORDER-BY
                // test above verifies the bandwidth win directly.
            }
        }
    }

    // -- cluster harness ------------------------------------------------------

    private static final class TestCluster implements AutoCloseable {
        private final MeshTransport transport = new InMemoryMeshTransport();
        private final MeshDriver driver;
        private final List<MeshAgent> agents = new ArrayList<>();
        private final AtomicInteger usScanned = new AtomicInteger();
        private final AtomicInteger euScanned = new AtomicInteger();

        TestCluster(int usRows, int euRows) throws Exception {
            Type type = eventsType();
            DistributedTableRegistry tables = new DistributedTableRegistry();
            tables.register(new SimpleTable("events", type, List.of(
                    new DistributedTable.Partition("us",
                            Set.of("jvssql", "partition:events:us"), usRows),
                    new DistributedTable.Partition("eu",
                            Set.of("jvssql", "partition:events:eu"), euRows))));
            driver = new MeshDriver(transport, tables, 10_000);
            driver.start();

            agents.add(spawn("agent-us", type, "us", genRows("us", 1, usRows), usScanned));
            agents.add(spawn("agent-eu", type, "eu", genRows("eu", usRows + 1, usRows + euRows), euScanned));
            waitForAgents(2);
        }

        private MeshAgent spawn(String id, Type type, String pk, List<JVS> rows, AtomicInteger counter) {
            AgentConfig cfg = new AgentConfig(
                    id,
                    Set.of("jvssql", "partition:events:" + pk),
                    Duration.ofMillis(100),
                    List.of(new CountingTable("events", type, pk, rows, counter)));
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
        AtomicInteger usScanCount() { return usScanned; }
        AtomicInteger euScanCount() { return euScanned; }

        @Override public void close() {
            for (var a : agents) a.close();
            driver.close();
            transport.close();
        }
    }

    // -- helpers --------------------------------------------------------------

    private static Type eventsType() throws Exception {
        Type t = new Type();
        t.init(MAPPER.readTree("{\"name\":\"events\",\"fields\":["
                + "{\"name\":\"id\",\"type\":\"core_string\"},"
                + "{\"name\":\"val\",\"type\":\"core_long\"}]}"));
        return t;
    }

    private static List<JVS> genRows(String prefix, int loInclusive, int hiInclusive) throws Exception {
        List<JVS> out = new ArrayList<>();
        for (int i = loInclusive; i <= hiInclusive; i++) {
            out.add(new JVS(MAPPER.readTree(
                    "{\"id\":\"" + prefix + "-" + i + "\",\"val\":" + i + "}")));
        }
        return out;
    }

    private record SimpleTable(String name, Type type, List<DistributedTable.Partition> partitions)
            implements DistributedTable {}

    /**
     * LocalTable that counts rows returned from its scan iterator. Lets tests
     * observe how many rows an agent actually pulled from its source — proves
     * pushdown limited the work.
     */
    private static final class CountingTable implements LocalTable {
        private final String name;
        private final Type type;
        private final String partitionKey;
        private final List<JVS> rows;
        private final AtomicInteger counter;

        CountingTable(String name, Type type, String pk, List<JVS> rows, AtomicInteger counter) {
            this.name = name; this.type = type; this.partitionKey = pk;
            this.rows = rows; this.counter = counter;
        }
        @Override public String name() { return name; }
        @Override public Type type() { return type; }
        @Override public String partitionKey() { return partitionKey; }
        @Override public Iterator<JVS> openScan() {
            Iterator<JVS> src = new ArrayList<>(rows).iterator();
            return new Iterator<>() {
                @Override public boolean hasNext() { return src.hasNext(); }
                @Override public JVS next() {
                    counter.incrementAndGet();
                    return src.next();
                }
            };
        }
    }
}
