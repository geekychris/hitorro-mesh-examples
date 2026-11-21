/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.examples;

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
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Pins distributed query results end to end to 3.0.2 on Jackson 2. An in-JVM cluster (driver and
 * one agent per partition over {@code InMemoryMeshTransport}, so every task and row goes through
 * mesh-core's message codec) runs 18 queries over {@link DocsByLanguageExample} and over a
 * three-partition dataset with wrongly typed and edge-case values. The queries cover filters,
 * projections, ORDER BY merges, LIMIT, GROUP BY with COUNT/SUM/MIN/MAX/AVG, HAVING and DISTINCT.
 * For each row the test records the JSON and the node/number type of every value; for failing
 * queries, the exception class. Rows of unordered queries are sorted.
 * {@code jackson2-golden.txt} was produced by running the same program against the 3.0.2 classes on
 * Jackson 2.18.
 */
class Jackson2GoldenCompatTest {
  static ObjectMapper M;

  /** The example dataset with wrongly typed and edge-case values, three partitions. */
  static final class MessyExample implements HitorroMeshExample {
    @Override public String name() { return "messy"; }
    @Override public String tableName() { return "docs"; }
    @Override public Type tableType() {
      try {
        Type t = new Type();
        t.init(M.readTree("{\"name\":\"docs\",\"fields\":[{\"name\":\"id\",\"type\":\"core_string\"},"
            + "{\"name\":\"title\",\"type\":\"core_string\"},{\"name\":\"lang\",\"type\":\"core_string\"},"
            + "{\"name\":\"size_kb\",\"type\":\"core_long\"},{\"name\":\"score\",\"type\":\"core_double\"}]}"));
        return t;
      } catch (Exception e) { throw new RuntimeException(e); }
    }
    static JVS j(String s) { try { return new JVS(M.readTree(s)); } catch (Exception e) { throw new RuntimeException(e); } }
    @Override public List<PartitionData> partitions() {
      return List.of(
        new PartitionData("p1", List.of(
          j("{\"id\":\"a\",\"title\":\"caf\\u00e9 \\ud83d\\ude00 \\\"q\\\" \\\\ /\",\"lang\":\"en\",\"size_kb\":100,\"score\":1.10}"),
          j("{\"id\":\"b\",\"title\":null,\"lang\":\"en\",\"size_kb\":12345678901,\"score\":-0.0}"),
          j("{\"id\":\"c\",\"lang\":\"fr\",\"size_kb\":3.7,\"score\":1e30}"))),
        new PartitionData("p2", List.of(
          j("{\"id\":\"d\",\"title\":\"\",\"lang\":\"fr\",\"size_kb\":\"300\",\"score\":\"7\"}"),
          j("{\"id\":\"e\",\"title\":{\"x\":[1,2]},\"lang\":null,\"size_kb\":null,\"score\":12345678901234567890}"),
          j("{\"id\":\"f\",\"title\":\"t\",\"lang\":\"en\",\"size_kb\":0,\"score\":2.5,\"extra\":[true,null]}"))),
        new PartitionData("p3", List.of(
          j("{\"id\":\"g\",\"title\":\"\\u0000\\u001f\",\"lang\":\"de\",\"size_kb\":-5,\"score\":0.1}"),
          j("{\"id\":7,\"title\":true,\"lang\":\"de\",\"size_kb\":250,\"score\":3}"))));
    }
    @Override public Map<String, String> queries() { return Map.of(); }
  }

  static final String[] QUERIES = {
    "SELECT * FROM docs",
    "SELECT id, title, lang, size_kb FROM docs",
    "SELECT id, score FROM docs",
    "SELECT id FROM docs WHERE lang = 'en'",
    "SELECT id, size_kb FROM docs WHERE size_kb > 200",
    "SELECT id, size_kb FROM docs ORDER BY size_kb",
    "SELECT id, size_kb FROM docs ORDER BY size_kb DESC LIMIT 3",
    "SELECT id, lang, size_kb FROM docs ORDER BY lang ASC, size_kb DESC",
    "SELECT id, title FROM docs ORDER BY title",
    "SELECT id, score FROM docs ORDER BY score DESC",
    "SELECT id FROM docs ORDER BY id",
    "SELECT lang, COUNT(*) FROM docs GROUP BY lang",
    "SELECT lang, SUM(size_kb), MIN(size_kb), MAX(size_kb) FROM docs GROUP BY lang",
    "SELECT lang, AVG(size_kb) FROM docs GROUP BY lang",
    "SELECT lang, COUNT(*) FROM docs GROUP BY lang HAVING COUNT(*) > 1",
    "SELECT COUNT(*) FROM docs",
    "SELECT DISTINCT lang FROM docs",
    "SELECT id FROM docs LIMIT 0"
  };

  static String sig(JsonNode n) {
    if (n == null) return "<java-null>";
    if (n.isObject()) {
      StringBuilder b = new StringBuilder("{");
      for (Map.Entry<String, JsonNode> e : n.properties()) { if (b.length() > 1) b.append(','); b.append(e.getKey()).append(':').append(sig(e.getValue())); }
      return b.append('}').toString();
    }
    if (n.isArray()) {
      StringBuilder b = new StringBuilder("[");
      for (JsonNode c : n) { if (b.length() > 1) b.append(','); b.append(sig(c)); }
      return b.append(']').toString();
    }
    if (n.isNumber()) return n.numberType() + "(" + n.toString() + ")";
    return n.getNodeType() + "(" + n.toString() + ")";
  }

  static String generate() throws Exception {
    M = com.hitorro.util.json.JsonMappers.json();
    StringBuilder out = new StringBuilder();
    HitorroMeshExample[] examples = {new DocsByLanguageExample(), new MessyExample()};
    for (HitorroMeshExample ex : examples) {
      try (Cluster c = new Cluster(ex)) {
        for (int i = 0; i < QUERIES.length; i++) {
          String k = ex.name() + ".q" + i;
          out.append(k).append(".sql=").append(QUERIES[i]).append('\n');
          boolean ordered = QUERIES[i].contains("ORDER BY");
          try (QueryDispatcher.QueryHandle h = c.driver.dispatcher().submit(QUERIES[i])) {
            List<JsonNode> rows = h.collect(10, TimeUnit.SECONDS);
            List<String> lines = new ArrayList<>();
            for (JsonNode r : rows) lines.add(M.writeValueAsString(r) + "  " + sig(r));
            if (!ordered) Collections.sort(lines);
            for (int r = 0; r < lines.size(); r++) out.append(k).append(".row").append(r).append('=').append(lines.get(r)).append('\n');
            out.append(k).append(".rows=").append(lines.size()).append('\n');
          } catch (Throwable t) {
            out.append(k).append(".error=!").append(t.getClass().getSimpleName()).append('\n');
          }
        }
      }
    }
    return out.toString();
  }

  @org.junit.jupiter.api.Test
  void matchesJackson2() throws Exception {
    java.util.Map<String, String> golden = new java.util.LinkedHashMap<>();
    try (java.io.InputStream in = getClass().getResourceAsStream("jackson2-golden.txt")) {
      for (String line : new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).split("\n")) {
        int eq = line.indexOf('=');
        if (eq > 0) golden.put(line.substring(0, eq), line.substring(eq + 1));
      }
    }
    java.util.Map<String, String> actual = new java.util.LinkedHashMap<>();
    for (String line : generate().split("\n")) {
      int eq = line.indexOf('=');
      if (eq > 0) actual.put(line.substring(0, eq), line.substring(eq + 1));
    }
    java.util.List<String> diffs = new java.util.ArrayList<>();
    for (String k : golden.keySet())
      if (!java.util.Objects.equals(golden.get(k), actual.get(k))) diffs.add(k + ": jackson2=" + golden.get(k) + " now=" + actual.get(k));
    for (String k : actual.keySet()) if (!golden.containsKey(k)) diffs.add(k + " (not in golden)");
    org.junit.jupiter.api.Assertions.assertTrue(diffs.isEmpty(), diffs.size() + " differences:\n" + String.join("\n", diffs));
    org.junit.jupiter.api.Assertions.assertEquals(247, golden.size());
  }

  // -- in-JVM cluster: the same harness as the end-to-end tests ------------------------------
  static final class Cluster implements AutoCloseable {
    final MeshTransport transport = new InMemoryMeshTransport();
    final MeshDriver driver;
    final List<MeshAgent> agents = new ArrayList<>();
    Cluster(HitorroMeshExample ex) throws Exception {
      DistributedTableRegistry tables = new DistributedTableRegistry();
      List<DistributedTable.Partition> parts = new ArrayList<>();
      for (var pd : ex.partitions())
        parts.add(new DistributedTable.Partition(pd.key(), Set.of("jvssql", "partition:" + ex.tableName() + ":" + pd.key()), pd.rows().size()));
      tables.register(new SimpleTable(ex.tableName(), ex.tableType(), parts));
      driver = new MeshDriver(transport, tables, 10_000);
      driver.start();
      for (var pd : ex.partitions()) {
        AgentConfig cfg = new AgentConfig("agent-" + pd.key(), Set.of("jvssql", "partition:" + ex.tableName() + ":" + pd.key()),
            Duration.ofMillis(100), List.of(new StaticTable(ex.tableName(), ex.tableType(), pd.key(), pd.rows())));
        MeshAgent a = new MeshAgent(transport, cfg);
        a.start();
        agents.add(a);
      }
      long deadline = System.currentTimeMillis() + 5_000;
      while (driver.agents().liveCount() < ex.partitions().size() && System.currentTimeMillis() < deadline) Thread.sleep(25);
    }
    @Override public void close() { for (var a : agents) a.close(); driver.close(); transport.close(); }
  }
  record SimpleTable(String name, Type type, List<DistributedTable.Partition> partitions) implements DistributedTable {}
  static final class StaticTable implements LocalTable {
    final String name; final Type type; final String partitionKey; final List<JVS> rows;
    StaticTable(String name, Type type, String pk, List<JVS> rows) { this.name = name; this.type = type; this.partitionKey = pk; this.rows = rows; }
    @Override public String name() { return name; }
    @Override public Type type() { return type; }
    @Override public String partitionKey() { return partitionKey; }
    @Override public Iterator<JVS> openScan() { return new ArrayList<>(rows).iterator(); }
  }
}
