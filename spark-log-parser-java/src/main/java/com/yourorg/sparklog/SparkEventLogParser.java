package com.yourorg.sparklog;

import com.fasterxml.jackson.databind.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class SparkEventLogParser {
  private static final ObjectMapper M = new ObjectMapper();
  private final Map<String, StageAgg> stages = new HashMap<>();
  private CompactParsedLog out;

  public CompactParsedLog parse(Path log) throws Exception {
    out = new CompactParsedLog();
    out.status = "UNKNOWN";

    try (BufferedReader br = open(log)) {
      String line;
      while ((line = br.readLine()) != null) {
        JsonNode e = M.readTree(line);
        if (!e.has("Event")) continue;

        switch (e.get("Event").asText()) {
          case "SparkListenerApplicationStart" -> out.startTimeMs = e.get("Timestamp").asLong();
          case "SparkListenerApplicationEnd" -> out.endTimeMs = e.get("Timestamp").asLong();
          case "SparkListenerTaskEnd" -> onTaskEnd(e);
        }
      }
    }

    finalizeStages();
    out.utilization = UtilizationScorer.score(out);
    if (out.endTimeMs != null) out.status = "SUCCEEDED";
    return out;
  }

  private BufferedReader open(Path p) throws IOException {
    InputStream in = Files.newInputStream(p);
    if (p.toString().endsWith(".gz")) in = new GZIPInputStream(in);
    return new BufferedReader(new InputStreamReader(in));
  }

  private void onTaskEnd(JsonNode e) {
    JsonNode si = e.get("Stage Info");
    if (si == null) return;
    String key = si.get("Stage ID").asInt() + ":" + si.get("Stage Attempt ID").asInt();
    StageAgg a = stages.computeIfAbsent(key, k -> new StageAgg());

    long launch = e.at("/Task Info/Launch Time").asLong(0);
    long finish = e.at("/Task Info/Finish Time").asLong(0);
    long dur = finish - launch;
    if (dur > 0) a.taskDurations.add(dur);

    JsonNode tm = e.get("Task Metrics");
    if (tm != null) a.executorRunTimeMs += tm.get("Executor Run Time").asLong(0);
  }

  private void finalizeStages() {
    for (StageAgg a : stages.values()) {
      CompactParsedLog.StageSummary s = new CompactParsedLog.StageSummary();
      long[] v = a.taskDurations.stream().mapToLong(x -> x).sorted().toArray();
      if (v.length > 0) {
        s.maxTaskDurationMs = v[v.length - 1];
        s.p50TaskDurationMs = v[v.length / 2];
      }
      s.executorRunTimeMs = a.executorRunTimeMs;
      out.stages.add(s);
    }
  }

  static class StageAgg {
    long executorRunTimeMs;
    List<Long> taskDurations = new ArrayList<>();
  }
}
