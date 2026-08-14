/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.mesh.examples;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Documents split across three partitions by originating region. Same shape,
 * different regional shards. Runs two phase-1 queries: one WHERE filter, one
 * WHERE + projection. Union of matching rows arrives at the driver.
 *
 * <p>Try it: {@code mvn -pl hitorro-mesh-examples exec:java
 * -Dexec.mainClass=com.hitorro.mesh.examples.DocsByLanguageExample}</p>
 */
public final class DocsByLanguageExample implements HitorroMeshExample {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        ExampleClusterRunner.run(new DocsByLanguageExample());
    }

    @Override public String name() { return "docs-by-language"; }
    @Override public String tableName() { return "docs"; }

    @Override public Type tableType() {
        String typeJson = "{\"name\":\"docs\",\"fields\":["
                + "{\"name\":\"id\",       \"type\":\"core_string\"},"
                + "{\"name\":\"title\",    \"type\":\"core_string\"},"
                + "{\"name\":\"lang\",     \"type\":\"core_string\"},"
                + "{\"name\":\"size_kb\",  \"type\":\"core_long\"}"
                + "]}";
        Type t = new Type();
        try { t.init(MAPPER.readTree(typeJson)); }
        catch (Exception e) { throw new RuntimeException(e); }
        return t;
    }

    @Override public List<PartitionData> partitions() {
        return List.of(
                new PartitionData("us", List.of(
                        row("us-1", "Quarterly Report",  "en", 210),
                        row("us-2", "Roadmap Q4",        "en", 84),
                        row("us-3", "Hiring Plan",       "en", 640)
                )),
                new PartitionData("eu", List.of(
                        row("eu-1", "Rapport Annuel",    "fr", 512),
                        row("eu-2", "Marktbericht",      "de", 128),
                        row("eu-3", "Product Overview",  "en", 45)
                )),
                new PartitionData("apac", List.of(
                        row("ap-1", "四半期報告",         "ja", 190),
                        row("ap-2", "월간 리뷰",           "ko", 55),
                        row("ap-3", "APAC Rollout",      "en", 320)
                ))
        );
    }

    @Override public Map<String, String> queries() {
        var q = new LinkedHashMap<String, String>();
        q.put("english-docs",
                "SELECT id, title FROM docs WHERE lang = 'en'");
        q.put("large-docs-any-lang",
                "SELECT id, title, lang, size_kb FROM docs WHERE size_kb > 200");
        // Rejected at plan time — sanity check that the guard fires:
        q.put("count-by-lang-BAD",
                "SELECT lang, COUNT(*) FROM docs GROUP BY lang");
        return q;
    }

    private JVS row(String id, String title, String lang, long sizeKb) {
        try {
            return new JVS(MAPPER.readTree(
                    "{\"id\":\"" + id + "\","
                  + "\"title\":\"" + title + "\","
                  + "\"lang\":\"" + lang + "\","
                  + "\"size_kb\":" + sizeKb + "}"));
        } catch (Exception e) { throw new RuntimeException(e); }
    }
}
