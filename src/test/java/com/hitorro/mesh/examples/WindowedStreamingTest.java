/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.examples;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.jvssql.config.StreamConfig;
import com.hitorro.mesh.InMemoryMeshTransport;
import com.hitorro.mesh.MeshTransport;
import com.hitorro.mesh.agent.AgentConfig;
import com.hitorro.mesh.agent.InMemoryStreamingTable;
import com.hitorro.mesh.agent.MeshAgent;
import com.hitorro.mesh.driver.DistributedTable;
import com.hitorro.mesh.driver.DistributedTableRegistry;
import com.hitorro.mesh.driver.MeshDriver;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 6d.1: watermark-driven windowed streaming aggregate.
 *
 * <p>A single agent holds a streaming partition. The driver query is a
 * windowed aggregate (GROUP BY {@code WIN_START(event_time, N)}). We push
 * events with progressively-advancing {@code event_time} — the closed
 * window's aggregate row should arrive at the driver BEFORE the stream
 * is stopped, proving watermark-driven emission works end-to-end through
 * the mesh transport.</p>
 *
 * <p>The mesh path this exercises:</p>
 * <ol>
 *   <li>{@link DistributedTable#streamConfig()} non-null → registry adds
 *       the table to {@code streamingTableNames()}.</li>
 *   <li>{@link com.hitorro.mesh.driver.QueryPlanner#plan} detects
 *       windowed aggregate + streaming table → returns
 *       {@code StreamingSimplePlan} instead of the batch
 *       {@code TwoStagePlan} (which would buffer forever).</li>
 *   <li>{@link com.hitorro.mesh.driver.QueryDispatcher} dispatches ONE
 *       scan task with the ORIGINAL SQL.</li>
 *   <li>Agent's {@link com.hitorro.mesh.agent.TaskExecutor} registers the
 *       source with {@link StreamConfig} (from
 *       {@link com.hitorro.mesh.agent.LocalTable#streamConfig()}), jvssql
 *       auto-swaps to {@code StreamingAggregate}, windows flush as the
 *       watermark advances.</li>
 * </ol>
 */
class WindowedStreamingTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final long WINDOW_MS = 60_000L;   // 1-minute tumbling window

    @Test
    void windowedStream_emitsClosedWindowIncrementally() throws Exception {
        try (var c = new TestCluster()) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT WIN_START(event_time, " + WINDOW_MS + ") AS ws, "
                    + "       COUNT(*) AS n "
                    + "FROM events "
                    + "GROUP BY WIN_START(event_time, " + WINDOW_MS + ")")) {

                // Establish dispatch + streaming registration.
                Thread.sleep(150);

                // Push two events in window 0. No emission yet — watermark
                // hasn't advanced past window-end (60_000).
                c.table().pushRow(event(0L,      "eng"));
                c.table().pushRow(event(30_000L, "eng"));

                // Push an event in window 1 (event_time = 65s). Watermark
                // advances to 65_000 → window 0 (ends at 60_000) closes and
                // its aggregate row emits.
                c.table().pushRow(event(65_000L, "eng"));

                // Should receive window-0 aggregate now (n=2), NOT waiting
                // for the stream to end.
                JsonNode w0 = h.nextRow(3, TimeUnit.SECONDS);
                assertThat(w0).as("closed window 0 should emit as soon as watermark passes 60s").isNotNull();
                assertThat(w0.get("ws").asLong()).isEqualTo(0L);
                assertThat(w0.get("n").asLong()).isEqualTo(2L);

                // Push an event in window 2 (event_time = 130s) → closes window 1.
                c.table().pushRow(event(130_000L, "eng"));

                JsonNode w1 = h.nextRow(3, TimeUnit.SECONDS);
                assertThat(w1).as("closed window 1 should emit after 130s row").isNotNull();
                assertThat(w1.get("ws").asLong()).isEqualTo(60_000L);
                assertThat(w1.get("n").asLong()).isEqualTo(1L);

                // Terminate the stream — jvssql flushes any remaining open
                // windows (window 2 with the 130s event) then EOS.
                c.table().stop();
                JsonNode w2 = h.nextRow(3, TimeUnit.SECONDS);
                assertThat(w2).isNotNull();
                assertThat(w2.get("ws").asLong()).isEqualTo(120_000L);
                assertThat(w2.get("n").asLong()).isEqualTo(1L);

                assertThat(h.nextRow(2, TimeUnit.SECONDS)).as("no more rows after stream stops").isNull();
            }
        }
    }

    @Test
    void multiPartitionStream_combinesPerWindowAcrossPartitions() throws Exception {
        // Phase 6d.2 — two partitions, both streaming. Windows close globally
        // when EVERY partition has emitted a row for a strictly later window
        // (advance-past heuristic). Driver reduces per-window partials using
        // the phase-2 combine SQL over an in-memory jvssql engine.
        try (var c = new MultiPartitionCluster()) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT WIN_START(event_time, " + WINDOW_MS + ") AS ws, "
                    + "       COUNT(*) AS n "
                    + "FROM events "
                    + "GROUP BY WIN_START(event_time, " + WINDOW_MS + ")")) {

                Thread.sleep(150);

                // Push events in window 0 to BOTH partitions.
                c.p1().pushRow(event(0L, "eng"));
                c.p1().pushRow(event(30_000L, "eng"));
                c.p2().pushRow(event(15_000L, "eng"));

                // Advance BOTH partitions' watermark to window 1.
                //   → each partition emits its own window-0 partial row
                //   → global min-latest = 60_000 > 0, so window 0 closes
                c.p1().pushRow(event(65_000L, "eng"));
                c.p2().pushRow(event(70_000L, "eng"));

                // Expect ONE combined row for window 0 with count = 3 (2 from p1 + 1 from p2).
                // Output columns use the user's SELECT aliases (ws, n) — phase 6d.2.3
                // preserves them through the combine SQL.
                JsonNode w0 = h.nextRow(3, TimeUnit.SECONDS);
                assertThat(w0).as("combined window-0 row should emit once both partitions advance").isNotNull();
                assertThat(w0.get("ws").asLong()).isEqualTo(0L);
                assertThat(w0.get("n").asLong()).as("2 events from p1 + 1 from p2").isEqualTo(3L);

                // Terminate both streams → any remaining buffered windows flush.
                c.p1().stop();
                c.p2().stop();

                // The (65_000, 70_000) events landed in window 1 (start=60_000).
                // On EOS both partitions' remaining open windows are flushed.
                JsonNode w1 = h.nextRow(3, TimeUnit.SECONDS);
                assertThat(w1).isNotNull();
                assertThat(w1.get("ws").asLong()).isEqualTo(60_000L);
                assertThat(w1.get("n").asLong()).as("1 event per partition in window 1").isEqualTo(2L);

                assertThat(h.nextRow(2, TimeUnit.SECONDS)).as("no more rows after both streams stopped").isNull();
            }
        }
    }

    @Test
    void watermarkHeartbeats_unblockWindowClosureForSparseEmitter() throws Exception {
        // Phase 6d.2.1: WATERMARK heartbeats let a partition close windows
        // WITHOUT emitting rows for them. Here p2 has events only in a
        // far-future window — it never emits for window 0. Without
        // heartbeats, driver would stall on window 0 (min-latest stuck at
        // MIN_VALUE for p2). With heartbeats, p2's watermark = 500_000
        // → latest-closed = 420_000, so window 0 (whose p1 contribution
        // is buffered) closes before EOS.
        try (var c = new MultiPartitionCluster()) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT WIN_START(event_time, " + WINDOW_MS + ") AS ws, "
                    + "       COUNT(*) AS n "
                    + "FROM events "
                    + "GROUP BY WIN_START(event_time, " + WINDOW_MS + ")")) {

                Thread.sleep(150);

                // p1 has events in window 0, plus one far-future event that
                // advances its watermark past window 0's boundary → jvssql
                // emits row for window 0 (count=2).
                c.p1().pushRow(event(0L, "eng"));
                c.p1().pushRow(event(30_000L, "eng"));
                c.p1().pushRow(event(500_000L, "eng"));

                // p2 has ONLY a far-future event — no data in window 0. jvssql
                // won't emit a row for window 0 on p2. Watermark = 500_000
                // means p2 has closed windows through 420_000. That's what the
                // WATERMARK heartbeat conveys.
                c.p2().pushRow(event(500_000L, "eng"));

                // Wait for the heartbeat to fire (200ms interval) and window 0
                // to emit. Give it a generous timeout — this proves the
                // emission happens BEFORE we stop the streams.
                JsonNode w0 = h.nextRow(3, TimeUnit.SECONDS);
                assertThat(w0).as("window 0 should close via p2's watermark heartbeat, "
                        + "even though p2 has no events in window 0").isNotNull();
                assertThat(w0.get("ws").asLong()).isEqualTo(0L);
                assertThat(w0.get("n").asLong()).as("only p1's 2 events, p2 contributed 0").isEqualTo(2L);

                c.p1().stop();
                c.p2().stop();

                // Drain: window 480_000 has 1 row from each partition (event
                // at 500_000). Watermark on both = 500_000 < 540_000, so
                // jvssql doesn't emit for window 480_000 during live streaming.
                // EOS triggers drain-all, jvssql flushes remaining windows.
                // Both partitions contribute 1 row for window 480_000 → combined 2.
                JsonNode w480 = h.nextRow(3, TimeUnit.SECONDS);
                assertThat(w480).isNotNull();
                assertThat(w480.get("ws").asLong()).isEqualTo(480_000L);
                assertThat(w480.get("n").asLong()).isEqualTo(2L);

                assertThat(h.nextRow(2, TimeUnit.SECONDS)).isNull();
            }
        }
    }

    // -- helpers -------------------------------------------------------------

    private static JVS event(long eventTime, String dept) throws Exception {
        return new JVS(MAPPER.readTree("{\"event_time\":" + eventTime
                + ",\"dept\":\"" + dept + "\"}"));
    }

    private static Type eventsType() throws Exception {
        Type t = new Type();
        t.init(MAPPER.readTree("{\"name\":\"events\",\"fields\":["
                + "{\"name\":\"event_time\",\"type\":\"core_long\"},"
                + "{\"name\":\"dept\",\"type\":\"core_string\"}]}"));
        return t;
    }

    // -- cluster harness -----------------------------------------------------

    private static final class TestCluster implements AutoCloseable {
        private final MeshTransport transport = new InMemoryMeshTransport();
        private final MeshDriver driver;
        private final List<MeshAgent> agents = new ArrayList<>();
        private final InMemoryStreamingTable streaming;

        TestCluster() throws Exception {
            Type eventsType = eventsType();
            StreamConfig streamCfg = StreamConfig.eventTime("event_time");
            streaming = new InMemoryStreamingTable("events", eventsType, "p1", streamCfg);

            DistributedTableRegistry tables = new DistributedTableRegistry();
            tables.register(new StreamingDistTable("events", eventsType, List.of(
                    new DistributedTable.Partition("p1",
                            Set.of("jvssql", "partition:events:p1"), -1)), streamCfg));

            driver = new MeshDriver(transport, tables, 10_000);
            driver.start();

            AgentConfig cfg = new AgentConfig("agent-p1",
                    Set.of("jvssql", "partition:events:p1"),
                    Duration.ofMillis(100),
                    List.of(streaming));
            MeshAgent a = new MeshAgent(transport, cfg);
            a.start();
            agents.add(a);

            waitForAgents(1);
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
        InMemoryStreamingTable table() { return streaming; }

        @Override public void close() {
            for (var a : agents) a.close();
            driver.close();
            transport.close();
        }
    }

    /** Driver-side {@code DistributedTable} that declares itself streaming. */
    private record StreamingDistTable(String name, Type type,
                                      List<DistributedTable.Partition> partitions,
                                      StreamConfig sc) implements DistributedTable {
        @Override public StreamConfig streamConfig() { return sc; }
    }

    /** Two-partition cluster for the phase-6d.2 cross-partition combine test. */
    private static final class MultiPartitionCluster implements AutoCloseable {
        private final MeshTransport transport = new InMemoryMeshTransport();
        private final MeshDriver driver;
        private final List<MeshAgent> agents = new ArrayList<>();
        private final InMemoryStreamingTable p1;
        private final InMemoryStreamingTable p2;

        MultiPartitionCluster() throws Exception {
            Type eventsType = eventsType();
            StreamConfig streamCfg = StreamConfig.eventTime("event_time");
            p1 = new InMemoryStreamingTable("events", eventsType, "p1", streamCfg);
            p2 = new InMemoryStreamingTable("events", eventsType, "p2", streamCfg);

            DistributedTableRegistry tables = new DistributedTableRegistry();
            tables.register(new StreamingDistTable("events", eventsType, List.of(
                    new DistributedTable.Partition("p1", Set.of("jvssql", "partition:events:p1"), -1),
                    new DistributedTable.Partition("p2", Set.of("jvssql", "partition:events:p2"), -1)
            ), streamCfg));

            driver = new MeshDriver(transport, tables, 10_000);
            driver.start();

            agents.add(spawn("agent-p1", p1));
            agents.add(spawn("agent-p2", p2));
            waitForAgents(2);
        }

        private MeshAgent spawn(String id, InMemoryStreamingTable table) {
            AgentConfig cfg = new AgentConfig(id,
                    Set.of("jvssql", "partition:events:" + table.partitionKey()),
                    Duration.ofMillis(100),
                    List.of(table));
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
        InMemoryStreamingTable p1() { return p1; }
        InMemoryStreamingTable p2() { return p2; }

        @Override public void close() {
            for (var a : agents) a.close();
            driver.close();
            transport.close();
        }
    }
}
