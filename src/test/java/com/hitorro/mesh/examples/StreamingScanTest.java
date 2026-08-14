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
import com.hitorro.mesh.driver.QueryDispatcher;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 6a: streaming source.
 *
 * <p>Two agents each hold a streaming partition of {@code events}. A driver
 * query runs against the union; the test pushes rows into each partition
 * mid-flight and verifies they arrive at the driver's {@link QueryHandle}
 * as they're published. Stopping the streams unblocks the agents' scan
 * iterators, which publish EOS as normal.</p>
 */
class StreamingScanTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void streamingScan_yieldsRowsAsTheyArrive() throws Exception {
        try (var c = new TestCluster()) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT id, region FROM events")) {

                // Give the streaming task time to be dispatched + subscription to establish.
                Thread.sleep(100);

                // Push some rows into each partition — driver's handle should see them.
                c.usTable().pushRow(row("us-1", "us"));
                c.euTable().pushRow(row("eu-1", "eu"));
                c.usTable().pushRow(row("us-2", "us"));

                // Collect three rows with a reasonable per-row timeout.
                List<JsonNode> collected = new ArrayList<>();
                for (int i = 0; i < 3; i++) {
                    JsonNode r = h.nextRow(2, TimeUnit.SECONDS);
                    assertThat(r).isNotNull();
                    collected.add(r);
                }
                assertThat(collected).extracting(r -> r.get("id").asText())
                        .containsExactlyInAnyOrder("us-1", "eu-1", "us-2");

                // Push one more, verify it arrives.
                c.euTable().pushRow(row("eu-2", "eu"));
                JsonNode fourth = h.nextRow(2, TimeUnit.SECONDS);
                assertThat(fourth).isNotNull();
                assertThat(fourth.get("id").asText()).isEqualTo("eu-2");

                // Terminate the streams — driver's iterator should end.
                c.usTable().stop();
                c.euTable().stop();
                JsonNode terminated = h.nextRow(2, TimeUnit.SECONDS);
                assertThat(terminated).as("iterator should end after both streams stopped").isNull();
            }
        }
    }

    @Test
    void streamingScan_withWhereFilter_onlyMatchingRowsPropagate() throws Exception {
        try (var c = new TestCluster()) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT id, region FROM events WHERE region = 'us'")) {
                Thread.sleep(100);
                c.usTable().pushRow(row("us-a", "us"));
                c.euTable().pushRow(row("eu-a", "eu"));   // filtered out
                c.usTable().pushRow(row("us-b", "us"));

                List<JsonNode> collected = new ArrayList<>();
                for (int i = 0; i < 2; i++) {
                    JsonNode r = h.nextRow(2, TimeUnit.SECONDS);
                    collected.add(r);
                }
                assertThat(collected).extracting(r -> r.get("id").asText())
                        .containsExactlyInAnyOrder("us-a", "us-b");

                c.usTable().stop();
                c.euTable().stop();
                assertThat(h.nextRow(2, TimeUnit.SECONDS)).isNull();
            }
        }
    }

    // -- cluster harness with streaming tables --------------------------------

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
            // Ensure streams are stopped so worker threads exit cleanly.
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
