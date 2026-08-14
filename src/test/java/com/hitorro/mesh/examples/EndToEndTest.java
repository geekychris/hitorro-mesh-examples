/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.examples;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.mesh.InMemoryMeshTransport;
import com.hitorro.mesh.MeshTransport;
import com.hitorro.mesh.agent.AgentConfig;
import com.hitorro.mesh.agent.LocalTable;
import com.hitorro.mesh.agent.MeshAgent;
import com.hitorro.mesh.driver.DistributedTable;
import com.hitorro.mesh.driver.DistributedTableRegistry;
import com.hitorro.mesh.driver.MeshDriver;
import com.hitorro.mesh.driver.QueryDispatcher;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end smoke test for phase 1: an in-JVM cluster with 3 agents each
 * holding a partition; the driver dispatches WHERE queries and merges rows.
 *
 * <p>Also pins down the "aggregate queries are rejected at plan time"
 * guardrail so we don't accidentally return wrong-answer per-partition
 * aggregates before phase 2 shuffle lands.</p>
 */
class EndToEndTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void distributedFilter_unionsAcrossPartitions() throws Exception {
        var example = new DocsByLanguageExample();
        try (var cluster = new TestCluster(example)) {
            try (QueryDispatcher.QueryHandle h = cluster.driver().dispatcher().submit(
                    "SELECT id FROM docs WHERE lang = 'en'")) {
                List<JsonNode> rows = h.collect(5, TimeUnit.SECONDS);
                List<String> ids = rows.stream().map(r -> r.get("id").asText()).sorted().toList();
                // English docs from all three partitions
                assertThat(ids).containsExactlyInAnyOrder("us-1", "us-2", "us-3", "eu-3", "ap-3");
            }
        }
    }

    @Test
    void distributedProject_carriesMultipleColumns() throws Exception {
        try (var cluster = new TestCluster(new DocsByLanguageExample())) {
            try (var h = cluster.driver().dispatcher().submit(
                    "SELECT id, title, size_kb FROM docs WHERE size_kb > 200")) {
                List<JsonNode> rows = h.collect(5, TimeUnit.SECONDS);
                assertThat(rows).allSatisfy(r -> {
                    assertThat(r.get("id").isTextual()).isTrue();
                    assertThat(r.get("title").isTextual()).isTrue();
                    assertThat(r.get("size_kb").asLong()).isGreaterThan(200);
                });
                assertThat(rows.stream().map(r -> r.get("id").asText()).sorted().toList())
                        .containsExactly("ap-3", "eu-1", "us-1", "us-3");
            }
        }
    }

    /**
     * The set of things that still hit the {@code HARD_DISALLOWED} guard has
     * shrunk with each phase. This test now only pins {@code UNION}, the last
     * shape the planner still rejects outright.
     */
    @Test
    void unsupportedShapesAreRejectedAtPlanTime() throws Exception {
        try (var cluster = new TestCluster(new DocsByLanguageExample())) {
            assertThatThrownBy(() -> cluster.driver().dispatcher().submit(
                    "SELECT * FROM docs UNION SELECT * FROM other"))
                .hasMessageContaining("does not yet support: UNION");
        }
    }

    @Test
    void unknownTable_failsFast() throws Exception {
        try (var cluster = new TestCluster(new DocsByLanguageExample())) {
            assertThatThrownBy(() -> cluster.driver().dispatcher().submit(
                    "SELECT id FROM missing_table WHERE lang = 'en'"))
                .hasMessageContaining("no distributed table registered under name: missing_table");
        }
    }

    // -- tiny cluster harness -------------------------------------------------

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
