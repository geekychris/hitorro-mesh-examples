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
 * Phase 4c: OUTER JOIN against a broadcast table.
 *
 * <p>The distributed {@code docs} table has some rows whose {@code lang}
 * doesn't appear in the broadcast {@code langs} dimension. LEFT OUTER
 * JOIN should preserve those unmatched left rows with a null-padded
 * right side. Inner JOIN drops them.</p>
 *
 * <p>Each agent runs its {@code SELECT ... LEFT JOIN ...} locally against
 * its docs partition + the full langs broadcast; jvssql handles OUTER
 * semantics correctly per-partition. Driver unions the results — same
 * flow as INNER JOIN (phase 4a).</p>
 */
class OuterJoinTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void leftJoin_preservesUnmatchedLeftRows_withNullRightSide() throws Exception {
        try (var c = new TestCluster()) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT docs.id, docs.lang, langs.name FROM docs " +
                    "LEFT JOIN langs ON docs.lang = langs.code")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                // All 4 docs appear (2 with matches, 2 unmatched):
                //   us-1 (lang=en, matched)
                //   us-2 (lang=xx, unmatched) → langs.name should be null
                //   eu-1 (lang=fr, matched)
                //   eu-2 (lang=zz, unmatched)
                assertThat(rows).hasSize(4);
                Map<String, JsonNode> byId = new HashMap<>();
                for (JsonNode r : rows) byId.put(r.get("id").asText(), r);
                assertThat(byId.get("us-1").get("name").asText()).isEqualTo("English");
                assertThat(byId.get("eu-1").get("name").asText()).isEqualTo("French");
                // Unmatched rows: name is null or missing
                JsonNode us2Name = byId.get("us-2").get("name");
                assertThat(us2Name == null || us2Name.isNull())
                        .as("us-2 has lang=xx (no match); expect null name, got %s", us2Name).isTrue();
                JsonNode eu2Name = byId.get("eu-2").get("name");
                assertThat(eu2Name == null || eu2Name.isNull())
                        .as("eu-2 has lang=zz (no match); expect null name, got %s", eu2Name).isTrue();
            }
        }
    }

    @Test
    void innerJoin_dropsUnmatchedRows() throws Exception {
        try (var c = new TestCluster()) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT docs.id FROM docs " +
                    "JOIN langs ON docs.lang = langs.code")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                // Sanity check: baseline INNER JOIN drops the 2 unmatched rows.
                assertThat(rows).extracting(r -> r.get("id").asText())
                        .containsExactlyInAnyOrder("us-1", "eu-1");
            }
        }
    }

    @Test
    void rightJoin_broadcast_preservesUnmatchedRightRows() throws Exception {
        // langs=[en, fr, de], docs=[us-1(en), us-2(xx), eu-1(fr), eu-2(zz)].
        // RIGHT JOIN preserves every lang; en → us-1, fr → eu-1, de → null.
        // Per-partition semantics: each partition runs SELECT ... RIGHT JOIN
        // locally, so BOTH partitions emit a de-with-null row. Driver unions
        // them → 2 de rows total (one per partition).
        try (var c = new TestCluster()) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT docs.id, langs.code, langs.name FROM docs " +
                    "RIGHT JOIN langs ON docs.lang = langs.code")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                // us partition: en→us-1, fr→null, de→null = 3 rows
                // eu partition: en→null, fr→eu-1, de→null = 3 rows
                assertThat(rows).hasSize(6);
                long dePairs = rows.stream()
                        .filter(r -> "de".equals(r.get("code").asText()))
                        .count();
                assertThat(dePairs).as("de appears once per partition (broadcast joined locally)").isEqualTo(2);
                long unmatched = rows.stream()
                        .filter(r -> {
                            JsonNode id = r.get("id");
                            return id == null || id.isNull();
                        }).count();
                assertThat(unmatched).as("4 lang×partition pairs with no matching doc").isEqualTo(4);
            }
        }
    }

    @Test
    void fullJoin_broadcast_preservesBothUnmatchedSides() throws Exception {
        try (var c = new TestCluster()) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT docs.id, docs.lang, langs.code FROM docs " +
                    "FULL JOIN langs ON docs.lang = langs.code")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                // Per partition: 2 docs × 3 langs = 6 possible pairs.
                //   us: en→us-1 matched; fr→null(right-only); de→null(right-only);
                //       us-2(xx)→null(left-only) = 4 rows
                //   eu: fr→eu-1 matched; en→null(right-only); de→null(right-only);
                //       eu-2(zz)→null(left-only) = 4 rows
                // Total = 8.
                assertThat(rows).hasSize(8);
                long docOnly = rows.stream()
                        .filter(r -> {
                            JsonNode id = r.get("id");
                            JsonNode code = r.get("code");
                            return id != null && !id.isNull() && (code == null || code.isNull());
                        }).count();
                long langOnly = rows.stream()
                        .filter(r -> {
                            JsonNode id = r.get("id");
                            return id == null || id.isNull();
                        }).count();
                assertThat(docOnly).as("us-2 + eu-2 have no matching lang").isEqualTo(2);
                assertThat(langOnly).as("2 per-partition unmatched right rows × 2 partitions").isEqualTo(4);
            }
        }
    }

    @Test
    void leftJoin_composesWithGroupBy_countsIncludeUnmatched() throws Exception {
        try (var c = new TestCluster()) {
            try (var h = c.driver().dispatcher().submit(
                    "SELECT docs.lang, COUNT(*) FROM docs " +
                    "LEFT JOIN langs ON docs.lang = langs.code " +
                    "GROUP BY docs.lang")) {
                var rows = h.collect(5, TimeUnit.SECONDS);
                Map<String, Long> byLang = new HashMap<>();
                for (JsonNode r : rows) byLang.put(r.get("lang").asText(), r.get("c0").asLong());
                // All 4 docs counted, one per lang (each lang has 1 doc):
                assertThat(byLang).containsExactlyInAnyOrderEntriesOf(Map.of(
                        "en", 1L, "xx", 1L, "fr", 1L, "zz", 1L));
            }
        }
    }

    // -- cluster harness ------------------------------------------------------

    private static final class TestCluster implements AutoCloseable {
        private final MeshTransport transport = new InMemoryMeshTransport();
        private final MeshDriver driver;
        private final List<MeshAgent> agents = new ArrayList<>();

        TestCluster() throws Exception {
            Type docsType = docsType();
            Type langsType = langsType();
            // German ("de") is intentionally in langs but no doc references it —
            // gives RIGHT / FULL joins an unmatched right row to preserve.
            List<JVS> langs = List.of(
                    row("{\"code\":\"en\",\"name\":\"English\"}"),
                    row("{\"code\":\"fr\",\"name\":\"French\"}"),
                    row("{\"code\":\"de\",\"name\":\"German\"}"));

            DistributedTableRegistry tables = new DistributedTableRegistry();
            tables.register(new SimpleTable("docs", docsType, List.of(
                    new DistributedTable.Partition("us",
                            Set.of("jvssql", "partition:docs:us"), 2),
                    new DistributedTable.Partition("eu",
                            Set.of("jvssql", "partition:docs:eu"), 2))));
            tables.registerBroadcast("langs");
            driver = new MeshDriver(transport, tables, 10_000);
            driver.start();

            List<JVS> usDocs = List.of(
                    row("{\"id\":\"us-1\",\"lang\":\"en\"}"),
                    row("{\"id\":\"us-2\",\"lang\":\"xx\"}"));
            List<JVS> euDocs = List.of(
                    row("{\"id\":\"eu-1\",\"lang\":\"fr\"}"),
                    row("{\"id\":\"eu-2\",\"lang\":\"zz\"}"));
            agents.add(spawn("agent-us", docsType, "us", usDocs, langsType, langs));
            agents.add(spawn("agent-eu", docsType, "eu", euDocs, langsType, langs));
            waitForAgents(2);
        }

        private MeshAgent spawn(String id, Type docsType, String pk, List<JVS> docs,
                                Type langsType, List<JVS> langs) {
            LocalTable partition = new StaticTable("docs", docsType, pk, docs);
            LocalTable broadcast = new StaticTable("langs", langsType, null, langs);
            AgentConfig cfg = new AgentConfig(
                    id,
                    Set.of("jvssql", "partition:docs:" + pk),
                    Duration.ofMillis(100),
                    List.of(partition),
                    List.of(broadcast));
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

    private static Type docsType() throws Exception {
        Type t = new Type();
        t.init(MAPPER.readTree("{\"name\":\"docs\",\"fields\":["
                + "{\"name\":\"id\",\"type\":\"core_string\"},"
                + "{\"name\":\"lang\",\"type\":\"core_string\"}]}"));
        return t;
    }

    private static Type langsType() throws Exception {
        Type t = new Type();
        t.init(MAPPER.readTree("{\"name\":\"langs\",\"fields\":["
                + "{\"name\":\"code\",\"type\":\"core_string\"},"
                + "{\"name\":\"name\",\"type\":\"core_string\"}]}"));
        return t;
    }

    private static JVS row(String json) throws Exception {
        return new JVS(MAPPER.readTree(json));
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
