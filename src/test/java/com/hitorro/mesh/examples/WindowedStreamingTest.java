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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    void multiPartitionStreaming_rejectedAtDispatchTime() throws Exception {
        // MVP scope: single-partition only for streaming aggregate. Two
        // partitions would need cross-partition combine (phase 6d.2).
        try (var c = new TestCluster(/*extraPartitions*/ true)) {
            assertThatThrownBy(() -> c.driver().dispatcher().submit(
                    "SELECT WIN_START(event_time, " + WINDOW_MS + ") AS ws, COUNT(*) AS n "
                    + "FROM events GROUP BY WIN_START(event_time, " + WINDOW_MS + ")"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("single-partition")
                    .hasMessageContaining("phase 6d.2");
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

        TestCluster() throws Exception { this(false); }

        TestCluster(boolean extraPartitions) throws Exception {
            Type eventsType = eventsType();
            StreamConfig streamCfg = StreamConfig.eventTime("event_time");
            streaming = new InMemoryStreamingTable("events", eventsType, "p1", streamCfg);

            DistributedTableRegistry tables = new DistributedTableRegistry();
            List<DistributedTable.Partition> parts = new ArrayList<>();
            parts.add(new DistributedTable.Partition("p1",
                    Set.of("jvssql", "partition:events:p1"), -1));
            if (extraPartitions) {
                parts.add(new DistributedTable.Partition("p2",
                        Set.of("jvssql", "partition:events:p2"), -1));
            }
            tables.register(new StreamingDistTable("events", eventsType, parts, streamCfg));

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
}
