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
 * Phase 6d: windowed aggregation via jvssql's {@code WIN_START(...)}.
 *
 * <p>Events are partitioned across two agents. The query buckets rows into
 * one-hour tumbling windows by {@code event_time} and counts per bucket.
 * Function-call group cols get auto-aliased ({@code g0}) so the combine
 * step can reference them — see {@code QueryPlanner} phase-6d design.</p>
 *
 * <p>Uses a batch source (rows pre-registered) rather than a streaming
 * source — windowed aggregation over a bounded input exercises the same
 * planner rewrite path with a simpler test shape. Once the mesh grows
 * watermark-aware combine, a streaming variant lands.</p>
 */
class WindowedAggregationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final long HOUR_MS = 3_600_000L;

    @Test
    void tumblingWindow_countByHour_combinesAcrossPartitions() throws Exception {
        try (var c = new TestCluster()) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT WIN_START(event_time, " + HOUR_MS + "), COUNT(*) "
                  + "FROM events GROUP BY WIN_START(event_time, " + HOUR_MS + ")")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                // Data spans 3 hour buckets:
                //   hour 0 (0..3.6M):  4 events (us: 3, eu: 1)
                //   hour 1 (3.6M..7.2M): 2 events (us: 1, eu: 1)
                //   hour 2 (7.2M..10.8M): 2 events (us: 0, eu: 2)
                Map<Long, Long> byWindow = new HashMap<>();
                for (JsonNode r : rows) byWindow.put(r.get("g0").asLong(), r.get("c0").asLong());
                assertThat(byWindow).containsExactlyInAnyOrderEntriesOf(Map.of(
                        0L,          4L,
                        HOUR_MS,     2L,
                        2 * HOUR_MS, 2L));
            }
        }
    }

    @Test
    void tumblingWindow_multipleGroupKeys_windowAndDept() throws Exception {
        try (var c = new TestCluster()) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT WIN_START(event_time, " + HOUR_MS + "), dept, COUNT(*) "
                  + "FROM events GROUP BY WIN_START(event_time, " + HOUR_MS + "), dept")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                // (window, dept) → count. Simple column ref `dept` keeps its
                // original name; function-call col gets auto-alias g0.
                Map<String, Long> byKey = new HashMap<>();
                for (JsonNode r : rows) {
                    String k = r.get("g0").asLong() + "|" + r.get("dept").asText();
                    byKey.put(k, r.get("c0").asLong());
                }
                // hour 0: eng × 3 (us-1,us-2,eu-1), sales × 1 (us-3)
                // hour 1: eng × 2 (us-4, eu-2)
                // hour 2: sales × 1 (eu-3), eng × 1 (eu-4)
                assertThat(byKey).contains(
                        Map.entry("0|eng", 3L),
                        Map.entry("0|sales", 1L),
                        Map.entry(HOUR_MS + "|eng", 2L),
                        Map.entry((2 * HOUR_MS) + "|sales", 1L),
                        Map.entry((2 * HOUR_MS) + "|eng", 1L));
            }
        }
    }

    @Test
    void tumblingWindow_orderByWindow_returnsChronologicalOrder() throws Exception {
        try (var c = new TestCluster()) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT WIN_START(event_time, " + HOUR_MS + "), COUNT(*) "
                  + "FROM events GROUP BY WIN_START(event_time, " + HOUR_MS + ") "
                  + "ORDER BY g0")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                assertThat(rows).hasSize(3);
                assertThat(rows.get(0).get("g0").asLong()).isEqualTo(0L);
                assertThat(rows.get(1).get("g0").asLong()).isEqualTo(HOUR_MS);
                assertThat(rows.get(2).get("g0").asLong()).isEqualTo(2 * HOUR_MS);
            }
        }
    }

    // -- cluster harness ------------------------------------------------------

    private static final class TestCluster implements AutoCloseable {
        private final MeshTransport transport = new InMemoryMeshTransport();
        private final MeshDriver driver;
        private final List<MeshAgent> agents = new ArrayList<>();

        TestCluster() throws Exception {
            Type eventsType = eventsType();
            DistributedTableRegistry tables = new DistributedTableRegistry();
            tables.register(new SimpleTable("events", eventsType, List.of(
                    new DistributedTable.Partition("us",
                            Set.of("jvssql", "partition:events:us"), 4),
                    new DistributedTable.Partition("eu",
                            Set.of("jvssql", "partition:events:eu"), 4))));
            driver = new MeshDriver(transport, tables, 10_000);
            driver.start();
            agents.add(spawn("agent-us", eventsType, "us", usRows()));
            agents.add(spawn("agent-eu", eventsType, "eu", euRows()));
            waitForAgents(2);
        }

        private MeshAgent spawn(String id, Type t, String pk, List<JVS> rows) {
            AgentConfig cfg = new AgentConfig(
                    id,
                    Set.of("jvssql", "partition:events:" + pk),
                    Duration.ofMillis(100),
                    List.of(new StaticTable("events", t, pk, rows)));
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

    // -- data + type ---------------------------------------------------------

    private static Type eventsType() throws Exception {
        Type t = new Type();
        t.init(MAPPER.readTree("{\"name\":\"events\",\"fields\":["
                + "{\"name\":\"id\",\"type\":\"core_string\"},"
                + "{\"name\":\"dept\",\"type\":\"core_string\"},"
                + "{\"name\":\"event_time\",\"type\":\"core_long\"}"
                + "]}"));
        return t;
    }

    /** us: 4 events across hour 0 (3), hour 1 (1). */
    private static List<JVS> usRows() throws Exception {
        return List.of(
            ev("us-1", "eng",   0),
            ev("us-2", "eng",   15 * 60_000L),
            ev("us-3", "sales", 45 * 60_000L),
            ev("us-4", "eng",   65 * 60_000L));   // hour 1
    }

    /** eu: 4 events across hour 0 (1), hour 1 (1), hour 2 (2). */
    private static List<JVS> euRows() throws Exception {
        return List.of(
            ev("eu-1", "eng",   30 * 60_000L),    // hour 0
            ev("eu-2", "eng",   90 * 60_000L),    // hour 1
            ev("eu-3", "sales", 130 * 60_000L),   // hour 2
            ev("eu-4", "eng",   150 * 60_000L));  // hour 2
    }

    private static JVS ev(String id, String dept, long eventTime) throws Exception {
        return new JVS(MAPPER.readTree(
                "{\"id\":\"" + id + "\",\"dept\":\"" + dept
                + "\",\"event_time\":" + eventTime + "}"));
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
