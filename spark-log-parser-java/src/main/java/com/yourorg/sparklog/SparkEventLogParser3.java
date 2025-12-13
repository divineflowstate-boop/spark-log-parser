package com.yourorg.sparklog;

import com.fasterxml.jackson.databind.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class SparkEventLogParser {
  private static final ObjectMapper M = new ObjectMapper();

  private static final List<String> CONF_PREFIXES = List.of(
      "spark.sql.adaptive.",
      "spark.sql.adaptive.skewJoin.",
      "spark.sql.shuffle.partitions",
      "spark.sql.autoBroadcastJoinThreshold",
      "spark.sql.files.maxPartitionBytes",
      "spark.sql.join.preferSortMergeJoin",
      "spark.dynamicAllocation.",
      "spark.executor.",
      "spark.sql.parquet.",
      "spark.sql.catalog.",
      "spark.sql.iceberg."
  );

  private final Map<String, StageAgg> stages = new HashMap<>();
  private final TreeMap<Long, Integer> execCountTimeline = new TreeMap<>();
  private int currentExecutors = 0;

  // Debug counters
  private long seenLines = 0;
  private long seenTaskEnd = 0;
  private long seenTaskEndMissingStage = 0;
  private long seenTaskEndMissingMetrics = 0;

  private CompactParsedLog out;

  public CompactParsedLog parse(Path eventLog) throws Exception {
    out = new CompactParsedLog();
    out.status = "UNKNOWN";

    try (BufferedReader br = open(eventLog)) {
      String line;
      while ((line = br.readLine()) != null) {
        seenLines++;
        if (line.isBlank()) continue;

        JsonNode e = M.readTree(line);
        JsonNode ev = e.get("Event");
        if (ev == null) continue;

        switch (ev.asText()) {
          case "SparkListenerApplicationStart" -> onAppStart(e);
          case "SparkListenerApplicationEnd" -> onAppEnd(e);
          case "SparkListenerEnvironmentUpdate" -> onEnvUpdate(e);

          case "SparkListenerExecutorAdded" -> onExecAdded(e);
          case "SparkListenerExecutorRemoved" -> onExecRemoved(e);

          case "SparkListenerStageSubmitted" -> onStageSubmitted(e);
          case "SparkListenerStageCompleted" -> onStageCompleted(e);

          case "SparkListenerTaskEnd" -> onTaskEnd(e);
          default -> { /* ignore */ }
        }
      }
    }

    finalizeStages();
    finalizeExecutorsAvg();
    finalizeStatus();

    out.utilization = UtilizationScorer.score(out);

    System.out.println("Parsed lines=" + seenLines
        + " TaskEnd=" + seenTaskEnd
        + " TaskEndMissingStage=" + seenTaskEndMissingStage
        + " TaskEndMissingMetrics=" + seenTaskEndMissingMetrics
        + " stagesBuilt=" + out.stages.size());

    return out;
  }

  private BufferedReader open(Path file) throws IOException {
    InputStream fis = Files.newInputStream(file);
    InputStream in = file.toString().endsWith(".gz") ? new GZIPInputStream(fis) : fis;
    return new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
  }

  private void onAppStart(JsonNode e) {
    out.appId = textAt(e, "App ID");
    out.startTimeMs = longAtOrNull(e, "Timestamp");
  }

  private void onAppEnd(JsonNode e) {
    out.endTimeMs = longAtOrNull(e, "Timestamp");
    if (out.startTimeMs != null && out.endTimeMs != null) {
      out.durationMs = out.endTimeMs - out.startTimeMs;
    }
  }

  private void onEnvUpdate(JsonNode e) {
    JsonNode props = e.get("Spark Properties");
    if (props == null || !props.isObject()) return;

    props.fields().forEachRemaining(kv -> {
      String k = kv.getKey();
      String v = kv.getValue().asText();
      if (keepConf(k)) out.sparkConf.put(k, v);
    });

    if (out.executors.executorCores == null) {
      String cores = out.sparkConf.get("spark.executor.cores");
      if (cores != null) {
        try { out.executors.executorCores = Integer.parseInt(cores.trim()); } catch (Exception ignored) {}
      }
    }
  }

  private void onExecAdded(JsonNode e) {
    long ts = longAt(e, "Timestamp", 0L);
    currentExecutors += 1;
    execCountTimeline.put(ts, currentExecutors);

    out.executors.totalAdded++;
    out.executors.maxExecutors = Math.max(out.executors.maxExecutors, currentExecutors);
  }

  private void onExecRemoved(JsonNode e) {
    long ts = longAt(e, "Timestamp", 0L);
    currentExecutors = Math.max(0, currentExecutors - 1);
    execCountTimeline.put(ts, currentExecutors);
    out.executors.totalRemoved++;
  }

  private void onStageSubmitted(JsonNode e) {
    JsonNode info = e.get("Stage Info");
    if (info == null) return;

    int stageId = intAt(info, "Stage ID");
    int attemptId = intAt(info, "Stage Attempt ID");

    StageAgg a = stages.computeIfAbsent(key(stageId, attemptId), k -> new StageAgg(stageId, attemptId));
    a.name = textAt(info, "Stage Name");
    a.submissionTimeMs = longAtOrNull(info, "Submission Time");
    a.numTasks = longAt(info, "Number of Tasks", a.numTasks);
  }

  private void onStageCompleted(JsonNode e) {
    JsonNode info = e.get("Stage Info");
    if (info == null) return;

    int stageId = intAt(info, "Stage ID");
    int attemptId = intAt(info, "Stage Attempt ID");

    StageAgg a = stages.computeIfAbsent(key(stageId, attemptId), k -> new StageAgg(stageId, attemptId));
    a.name = textAt(info, "Stage Name");
    a.submissionTimeMs = longAtOrNull(info, "Submission Time");
    a.completionTimeMs = longAtOrNull(info, "Completion Time");
    if (a.submissionTimeMs != null && a.completionTimeMs != null) a.durationMs = a.completionTimeMs - a.submissionTimeMs;

    Long nt = longAtOrNull(info, "Number of Tasks");
    if (nt != null) a.numTasks = nt;
  }

  // Spark 3.5.x TaskEnd: Stage ID is usually TOP-LEVEL, not in "Stage Info".
  private void onTaskEnd(JsonNode e) {
    seenTaskEnd++;

    Integer stageId = intAtOrNull(e, "Stage ID");
    Integer attemptId = intAtOrNull(e, "Stage Attempt ID");

    if (stageId == null || attemptId == null) {
      JsonNode stageInfo = e.get("Stage Info");
      if (stageInfo != null) {
        stageId = intAtOrNull(stageInfo, "Stage ID");
        attemptId = intAtOrNull(stageInfo, "Stage Attempt ID");
      }
    }

    if (stageId == null || attemptId == null) {
      seenTaskEndMissingStage++;
      return;
    }

    StageAgg a = stages.computeIfAbsent(key(stageId, attemptId), k -> new StageAgg(stageId, attemptId));

    long launch = longAt(e, "Task Info", "Launch Time", 0L);
    long finish = longAt(e, "Task Info", "Finish Time", 0L);
    long taskDur = (launch > 0 && finish > 0) ? (finish - launch) : 0L;
    if (taskDur > 0) a.taskDurations.add(taskDur);

    JsonNode tm = e.get("Task Metrics");
    if (tm == null || !tm.isObject()) {
      seenTaskEndMissingMetrics++;
      return;
    }

    a.executorRunTimeMs += longAt(tm, "Executor Run Time", 0L);
    a.gcTimeMs += longAt(tm, "JVM GC Time", 0L);

    a.spillMemBytes += longAt(tm, "Memory Bytes Spilled", 0L);
    a.spillDiskBytes += longAt(tm, "Disk Bytes Spilled", 0L);

    JsonNode sr = tm.get("Shuffle Read Metrics");
    if (sr != null && sr.isObject()) {
      a.shuffleReadBytes += longAt(sr, "Remote Bytes Read", 0L) + longAt(sr, "Local Bytes Read", 0L);
    }

    JsonNode sw = tm.get("Shuffle Write Metrics");
    if (sw != null && sw.isObject()) {
      a.shuffleWriteBytes += longAt(sw, "Shuffle Bytes Written", 0L);
    }
  }

  private void finalizeStages() {
    for (StageAgg a : stages.values()) {
      CompactParsedLog.StageSummary s = new CompactParsedLog.StageSummary();
      s.stageId = a.stageId;
      s.attemptId = a.attemptId;
      s.name = a.name;

      s.submissionTimeMs = a.submissionTimeMs;
      s.completionTimeMs = a.completionTimeMs;
      s.durationMs = a.durationMs;

      s.numTasks = a.numTasks;

      s.executorRunTimeMs = a.executorRunTimeMs;
      s.gcTimeMs = a.gcTimeMs;

      s.shuffleReadBytes = a.shuffleReadBytes;
      s.shuffleWriteBytes = a.shuffleWriteBytes;

      s.spillMemBytes = a.spillMemBytes;
      s.spillDiskBytes = a.spillDiskBytes;

      SkewStats st = SkewStats.from(a.taskDurations);
      s.maxTaskDurationMs = st.max;
      s.p50TaskDurationMs = st.p50;
      s.p95TaskDurationMs = st.p95;
      s.maxOverP50 = (st.p50 > 0) ? (st.max / st.p50) : 0.0;
      s.stragglerPct = st.stragglerPct;

      out.stages.add(s);
    }

    out.stages.sort(Comparator.comparingInt((CompactParsedLog.StageSummary x) -> x.stageId)
        .thenComparingInt(x -> x.attemptId));
  }

  private void finalizeExecutorsAvg() {
    if (out.startTimeMs == null || out.endTimeMs == null || out.endTimeMs <= out.startTimeMs) return;

    long start = out.startTimeMs;
    long end = out.endTimeMs;

    execCountTimeline.putIfAbsent(start,
        execCountTimeline.floorEntry(start) != null ? execCountTimeline.floorEntry(start).getValue() : 0);

    long area = 0, lastT = start;
    int lastC = execCountTimeline.floorEntry(start).getValue();

    for (var ent : execCountTimeline.subMap(start, true, end, true).entrySet()) {
      long t = ent.getKey();
      int c = ent.getValue();
      if (t > lastT) area += (t - lastT) * (long) lastC;
      lastT = t;
      lastC = c;
    }
    if (end > lastT) area += (end - lastT) * (long) lastC;

    out.executors.avgExecutors = (double) area / (double) (end - start);
  }

  private void finalizeStatus() {
    if (out.endTimeMs != null && "UNKNOWN".equals(out.status)) out.status = "SUCCEEDED";
  }

  // ---------- helpers ----------

  private static boolean keepConf(String k) {
    for (String p : CONF_PREFIXES) if (k.startsWith(p)) return true;
    return false;
  }

  private static String key(int stageId, int attemptId) { return stageId + ":" + attemptId; }

  private static String textAt(JsonNode n, String f) {
    JsonNode x = n.get(f);
    return x != null && !x.isNull() ? x.asText() : null;
  }

  private static Long longAtOrNull(JsonNode n, String f) {
    JsonNode x = n.get(f);
    return x != null && x.isNumber() ? x.asLong() : null;
  }

  private static long longAt(JsonNode n, String f, long def) {
    JsonNode x = n.get(f);
    return x != null && x.isNumber() ? x.asLong() : def;
  }

  private static long longAt(JsonNode n, String obj, String f, long def) {
    JsonNode o = n.get(obj);
    if (o == null) return def;
    JsonNode x = o.get(f);
    return x != null && x.isNumber() ? x.asLong() : def;
  }

  private static int intAt(JsonNode n, String f) {
    JsonNode x = n.get(f);
    return x != null ? x.asInt() : 0;
  }

  private static Integer intAtOrNull(JsonNode n, String f) {
    JsonNode x = n.get(f);
    return (x != null && x.isNumber()) ? x.asInt() : null;
  }

  // ---------- reducers ----------

  private static class StageAgg {
    final int stageId, attemptId;
    String name;

    Long submissionTimeMs, completionTimeMs, durationMs;
    long numTasks;

    long executorRunTimeMs, gcTimeMs;
    long shuffleReadBytes, shuffleWriteBytes;
    long spillMemBytes, spillDiskBytes;

    final Reservoir taskDurations = new Reservoir(15000);

    StageAgg(int stageId, int attemptId) { this.stageId = stageId; this.attemptId = attemptId; }
  }

  private static class Reservoir {
    private final long[] buf;
    private int size = 0;
    private long seen = 0;
    private final Random rnd = new Random(1);

    Reservoir(int cap) { buf = new long[cap]; }

    void add(long v) {
      seen++;
      if (size < buf.length) { buf[size++] = v; return; }
      long j = Math.abs(rnd.nextLong()) % seen;
      if (j < buf.length) buf[(int) j] = v;
    }

    long[] values() { return Arrays.copyOf(buf, size); }
  }

  private static class SkewStats {
    long max;
    double p50, p95;
    double stragglerPct;

    static SkewStats from(Reservoir r) {
      long[] v = r.values();
      SkewStats s = new SkewStats();
      if (v.length == 0) return s;

      Arrays.sort(v);
      s.max = v[v.length - 1];
      s.p50 = v[(int) Math.floor(0.50 * (v.length - 1))];
      s.p95 = v[(int) Math.floor(0.95 * (v.length - 1))];

      double thr = s.p50 * 3.0;
      long str = 0;
      for (long x : v) if (x > thr) str++;
      s.stragglerPct = (double) str / (double) v.length;
      return s;
    }
  }
}
