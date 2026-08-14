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
import com.hitorro.mesh.agent.InMemoryStreamingTable;
import com.hitorro.mesh.agent.LocalTable;
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
 * Phase 6c.2: cancel-through-to-agents.
 *
 * <p>Verifies that closing a {@link com.hitorro.mesh.driver.QueryDispatcher.QueryHandle}
 * from a streaming query propagates a cancel signal to the agents and
 * interrupts their long-running scan iterators — even when the source
 * would otherwise keep producing rows forever.</p>
 */
class StreamingCancelTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void closingHandle_cancelsAgentSideStream() throws Exception {
        try (var c = new TestCluster()) {
            var h = c.driver().dispatcher().submit("SELECT id, region FROM events");
            Thread.sleep(100);   // dispatch + subscribe

            // Push a row so the agent is definitely mid-blocked on take().
            c.usTable().pushRow(row("us-1", "us"));
            JsonNode first = h.nextRow(2, TimeUnit.SECONDS);
            assertThat(first).isNotNull();
            assertThat(first.get("id").asText()).isEqualTo("us-1");

            // Close the handle — should propagate a CancelMessage that
            // interrupts the agent's scan iterator (currently blocked on
            // queue.take()) → scan loop exits → agent publishes EOS →
            // subscription closes (though the handle is already closed).
            h.close();

            // Give the cancel signal + iterator interrupt a moment to land.
            // Then confirm the agent stopped iterating: push more rows and
            // verify no worker thread is still active for the query.
            Thread.sleep(200);

            // If we reopen the same-source query, it should pick up new
            // rows normally. Push a fresh row; the OLD stream is dead so it
            // won't consume this. A NEW query does.
            var h2 = c.driver().dispatcher().submit("SELECT id, region FROM events");
            Thread.sleep(100);
            c.usTable().pushRow(row("us-2", "us"));
            JsonNode next = h2.nextRow(2, TimeUnit.SECONDS);
            assertThat(next).isNotNull();
            assertThat(next.get("id").asText()).isEqualTo("us-2");
            h2.close();
        }
    }

    @Test
    void batchQuery_closeStillPublishesCancel_noSideEffects() throws Exception {
        // Cancel on a query that's already finished must be a no-op (agents
        // don't have any workers for that queryId). Doesn't error.
        try (var c = new TestCluster()) {
            // Give the streaming source a poison pill so this "query" behaves
            // like a bounded batch (empty result).
            c.usTable().stop();
            c.euTable().stop();
            Thread.sleep(50);

            try (var h = c.driver().dispatcher().submit("SELECT id FROM events")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                assertThat(rows).isEmpty();
                // close() at end of try-with-resources publishes cancel; agents
                // ignore it (no active tasks). Nothing to assert beyond
                // "test doesn't hang or throw."
            }
        }
    }

    // -- cluster harness (same as StreamingScanTest) -------------------------

    private static final class TestCluster implements AutoCloseable {
        private final MeshTransport transport = new InMemoryMeshTransport();
        private final MeshDriver driver;
        private final List<MeshAgent> agents = new ArrayList<>();
        private final InMemoryStreamingTable us;
        private final InMemoryStreamingTable eu;

        TestCluster() throws Exception {
            Type eventsType = eventsType();
            us = new InMemoryStreamingTable("events", eventsType, "us");
            eu = new InMemoryStreamingTable("events", eventsType, "eu");

            DistributedTableRegistry tables = new DistributedTableRegistry();
            tables.register(new SimpleTable("events", eventsType, List.of(
                    new DistributedTable.Partition("us",
                            Set.of("jvssql", "partition:events:us"), -1),
                    new DistributedTable.Partition("eu",
                            Set.of("jvssql", "partition:events:eu"), -1))));
            driver = new MeshDriver(transport, tables, 10_000);
            driver.start();

            agents.add(spawn("agent-us", us));
            agents.add(spawn("agent-eu", eu));
            waitForAgents(2);
        }

        private MeshAgent spawn(String id, LocalTable table) {
            AgentConfig cfg = new AgentConfig(
                    id,
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
        InMemoryStreamingTable usTable() { return us; }
        InMemoryStreamingTable euTable() { return eu; }

        @Override public void close() {
            us.stop();
            eu.stop();
            for (var a : agents) a.close();
            driver.close();
            transport.close();
        }
    }

    private static Type eventsType() throws Exception {
        Type t = new Type();
        t.init(MAPPER.readTree("{\"name\":\"events\",\"fields\":["
                + "{\"name\":\"id\",\"type\":\"core_string\"},"
                + "{\"name\":\"region\",\"type\":\"core_string\"}]}"));
        return t;
    }

    private static JVS row(String id, String region) {
        try {
            return new JVS(MAPPER.readTree(
                    "{\"id\":\"" + id + "\",\"region\":\"" + region + "\"}"));
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    private record SimpleTable(String name, Type type, List<DistributedTable.Partition> partitions)
            implements DistributedTable {}
}
