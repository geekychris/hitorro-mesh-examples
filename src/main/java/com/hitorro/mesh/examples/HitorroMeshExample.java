/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.examples;

import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;

import java.util.List;
import java.util.Map;

/**
 * A runnable mesh example.
 *
 * <p>Implementations declare:</p>
 * <ul>
 *   <li>Table shape ({@link #tableName()}, {@link #tableType()})</li>
 *   <li>Sample rows per partition ({@link #partitions()})</li>
 *   <li>Canned SQL queries to run against the cluster ({@link #queries()})</li>
 * </ul>
 *
 * <p>{@link ExampleClusterRunner} discovers implementations (currently by
 * classname CLI arg — a ServiceLoader hook is a phase-1.5 nice-to-have),
 * spins up an in-JVM mesh with one agent per partition, and runs each query.</p>
 */
public interface HitorroMeshExample {

    /** Short, kebab-case name; used as CLI selector. */
    String name();

    String tableName();

    Type tableType();

    /** Rows per partition — each entry becomes one agent's local data. */
    List<PartitionData> partitions();

    /** {@code queryName → SQL}. Kept in a Map so listings/CLI can pick one by name. */
    Map<String, String> queries();

    record PartitionData(String key, List<JVS> rows) {}
}
