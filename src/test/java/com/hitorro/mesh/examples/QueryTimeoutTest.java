/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.examples;

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
import com.hitorro.mesh.driver.QueryDispatcher;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 7b — query-level deadline enforcement.
 *
 * <p>A streaming query that would otherwise run forever should stop
 * cleanly when its timeout fires: {@link QueryDispatcher.QueryHandle#timedOut()}
 * flips true, further {@code nextRow} calls return {@code null}, and a
 * {@code CancelMessage} propagates to the agents so they stop
 * processing.</p>
 */
class QueryTimeoutTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void submitWithTimeout_neverCompletingStream_fires() throws Exception {
        // Streaming source that we never push rows to nor stop → the query
        // would block indefinitely without a deadline.
        try (var c = new TestCluster()) {
            long t0 = System.currentTimeMillis();
            try (QueryDispatcher.QueryHandle h = c.driver().dispatcher().submit(
                    "SELECT id FROM events", Duration.ofMillis(500))) {

                assertThat(h.timedOut()).as("not timed out immediately").isFalse();

                // Poll for rows — should return null once the deadline fires
                // and the handle is closed. Give it a bit longer than the
                // deadline itself to account for scheduler latency.
                var row = h.nextRow(2000, TimeUnit.MILLISECONDS);
                long elapsed = System.currentTimeMillis() - t0;

                assertThat(row).as("no rows should arrive before or after deadline").isNull();
                assertThat(h.timedOut()).as("timeout flag should be set after deadline fired").isTrue();
                assertThat(elapsed).as("should return within a small delta of the deadline")
                        .isBetween(400L, 1500L);
            }
        }
    }

    @Test
    void submitWithTimeout_completesBeforeDeadline_noTimeoutFlag() throws Exception {
        // Push rows immediately + stop the stream. Query completes naturally
        // way before the 5-second deadline; timeout flag should stay false.
        try (var c = new TestCluster()) {
            c.table().pushRow(row("a"));
            c.table().pushRow(row("b"));
            c.table().stop();

            try (QueryDispatcher.QueryHandle h = c.driver().dispatcher().submit(
                    "SELECT id FROM events", Duration.ofSeconds(5))) {
                var rows = h.collect(3, TimeUnit.SECONDS);
                assertThat(rows).hasSize(2);
                assertThat(h.timedOut()).as("natural completion → no timeout").isFalse();
            }
        }
    }

    @Test
    void submitWithoutTimeout_stillWorks_backCompat() throws Exception {
        try (var c = new TestCluster()) {
            c.table().pushRow(row("x"));
            c.table().stop();
            try (QueryDispatcher.QueryHandle h = c.driver().dispatcher().submit(
                    "SELECT id FROM events")) {
                var rows = h.collect(3, TimeUnit.SECONDS);
                assertThat(rows).hasSize(1);
                assertThat(h.timedOut()).isFalse();
            }
        }
    }

    // -- helpers -------------------------------------------------------------

    private static JVS row(String id) throws Exception {
        return new JVS(MAPPER.readTree("{\"id\":\"" + id + "\"}"));
    }

    private static Type eventsType() throws Exception {
        Type t = new Type();
        t.init(MAPPER.readTree("{\"name\":\"events\",\"fields\":["
                + "{\"name\":\"id\",\"type\":\"core_string\"}]}"));
        return t;
    }

    private static final class TestCluster implements AutoCloseable {
        private final MeshTransport transport = new InMemoryMeshTransport();
        private final MeshDriver driver;
        private final List<MeshAgent> agents = new ArrayList<>();
        private final InMemoryStreamingTable events;

        TestCluster() throws Exception {
            Type et = eventsType();
            events = new InMemoryStreamingTable("events", et, "p1");

            DistributedTableRegistry tables = new DistributedTableRegistry();
            tables.register(new SimpleDistTable("events", et, List.of(
                    new DistributedTable.Partition("p1",
                            Set.of("jvssql", "partition:events:p1"), -1))));

            driver = new MeshDriver(transport, tables, 10_000);
            driver.start();

            AgentConfig cfg = new AgentConfig("agent-p1",
                    Set.of("jvssql", "partition:events:p1"),
                    Duration.ofMillis(100), List.of(events));
            MeshAgent a = new MeshAgent(transport, cfg);
            a.start();
            agents.add(a);

            long deadline = System.currentTimeMillis() + 2_000;
            while (System.currentTimeMillis() < deadline && driver.agents().liveCount() < 1) {
                Thread.sleep(25);
            }
        }

        MeshDriver driver() { return driver; }
        InMemoryStreamingTable table() { return events; }

        @Override public void close() {
            for (var a : agents) a.close();
            driver.close();
            transport.close();
        }
    }

    private record SimpleDistTable(String name, Type type,
                                   List<DistributedTable.Partition> partitions)
            implements DistributedTable {}
}
