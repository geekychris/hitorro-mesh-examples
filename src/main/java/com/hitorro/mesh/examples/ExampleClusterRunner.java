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
import com.hitorro.mesh.driver.QueryDispatcher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Boots a full mesh inside one JVM: one shared {@link InMemoryMeshTransport},
 * one {@link MeshDriver}, one {@link MeshAgent} per partition. Runs the
 * example's queries against it, prints results, cleans up.
 *
 * <p>Zero external dependencies — good for onboarding, CI, and demos. When
 * we ship {@code NatsMeshTransport}, this class swaps the transport
 * constructor and everything else stays the same.</p>
 *
 * <p>Usage:</p>
 * <pre>
 *   ExampleClusterRunner.run(new DocsByLanguageExample());
 * </pre>
 */
public final class ExampleClusterRunner {

    public static void run(HitorroMeshExample example) throws Exception {
        System.out.println("== " + example.name() + " ==");
        System.out.println("  table: " + example.tableName() + "  partitions: "
                + example.partitions().stream().map(HitorroMeshExample.PartitionData::key).toList());

        try (MeshTransport transport = new InMemoryMeshTransport()) {

            // 1. Distributed table view for the driver — one partition entry per shard.
            DistributedTableRegistry tables = new DistributedTableRegistry();
            List<DistributedTable.Partition> parts = new ArrayList<>();
            for (var pd : example.partitions()) {
                Set<String> caps = Set.of(
                        "jvssql",
                        "partition:" + example.tableName() + ":" + pd.key());
                parts.add(new DistributedTable.Partition(pd.key(), caps, pd.rows().size()));
            }
            tables.register(new SimpleDistributedTable(example.tableName(), example.tableType(), parts));

            // 2. Driver.
            MeshDriver driver = new MeshDriver(transport, tables, 10_000);
            driver.start();

            // 3. One agent per partition, each holding just its shard.
            List<MeshAgent> agents = new ArrayList<>();
            for (var pd : example.partitions()) {
                LocalTable lt = new InMemoryLocalTable(example.tableName(), example.tableType(), pd.key(), pd.rows());
                AgentConfig cfg = new AgentConfig(
                        "agent-" + pd.key(),
                        Set.of("jvssql", "partition:" + example.tableName() + ":" + pd.key()),
                        Duration.ofMillis(200),
                        List.of(lt));
                MeshAgent agent = new MeshAgent(transport, cfg);
                agent.start();
                agents.add(agent);
            }

            // 4. Wait for agents to be visible in the driver's registry.
            waitForAgents(driver, agents.size(), Duration.ofSeconds(2));

            // 5. Run each query.
            for (var e : example.queries().entrySet()) {
                System.out.println();
                System.out.println("  query [" + e.getKey() + "] " + e.getValue());
                try (QueryDispatcher.QueryHandle handle = driver.dispatcher().submit(e.getValue())) {
                    List<JsonNode> rows = handle.collect(5, TimeUnit.SECONDS);
                    System.out.println("  → " + rows.size() + " row(s) from agents "
                            + handle.assignedAgents());
                    for (JsonNode r : rows) System.out.println("    " + r);
                } catch (Exception ex) {
                    System.out.println("  ✗ " + ex.getMessage());
                }
            }

            // 6. Shutdown.
            for (MeshAgent a : agents) a.close();
            driver.close();
        }
    }

    private static void waitForAgents(MeshDriver driver, int expected, Duration timeout) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (driver.agents().liveCount() >= expected) return;
            Thread.sleep(50);
        }
        throw new IllegalStateException("timed out waiting for " + expected + " agents; saw " + driver.agents().liveCount());
    }

    // -- helper types ---------------------------------------------------------

    private record SimpleDistributedTable(String name, Type type, List<DistributedTable.Partition> partitions)
            implements DistributedTable {}

    private static final class InMemoryLocalTable implements LocalTable {
        private final String name;
        private final Type type;
        private final String partitionKey;
        private final List<JVS> rows;

        InMemoryLocalTable(String name, Type type, String partitionKey, List<JVS> rows) {
            this.name = name;
            this.type = type;
            this.partitionKey = partitionKey;
            this.rows = rows;
        }

        @Override public String name() { return name; }
        @Override public Type type() { return type; }
        @Override public String partitionKey() { return partitionKey; }
        @Override public Iterator<JVS> openScan() { return new ArrayList<>(rows).iterator(); }
    }
}
