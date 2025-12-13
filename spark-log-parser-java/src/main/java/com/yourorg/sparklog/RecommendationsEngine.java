package com.yourorg.sparklog;

import java.util.*;

public class RecommendationsEngine {

  public static void enrich(CompactParsedLog run) {
    run.runInsights.totalStages = run.stages.size();

    long totalTasks = 0;
    List<Long> stageDur = new ArrayList<>();
    long tiny = 0;

    long totalExecutorRun = 0;
    long totalGc = 0;
    long totalSpill = 0;

    List<CompactParsedLog.StageSummary> byExec = new ArrayList<>(run.stages);
    byExec.sort((a, b) -> Long.compare(b.executorRunTimeMs, a.executorRunTimeMs));

    for (CompactParsedLog.StageSummary s : run.stages) {
      totalTasks += s.numTasks;
      if (s.durationMs != null) {
        stageDur.add(s.durationMs);
        if (s.durationMs < 200) tiny++;
      }
      totalExecutorRun += s.executorRunTimeMs;
      totalGc += s.gcTimeMs;
      totalSpill += (s.spillMemBytes + s.spillDiskBytes);
    }

    run.runInsights.totalTasks = totalTasks;
    run.runInsights.tinyStages = tiny;
    run.runInsights.medianStageDurationMs = median(stageDur);

    run.runInsights.evidence.put("totalTasks", totalTasks);
    run.runInsights.evidence.put("totalStages", run.runInsights.totalStages);
    run.runInsights.evidence.put("tinyStages(<200ms)", tiny);
    run.runInsights.evidence.put("medianStageDurationMs", run.runInsights.medianStageDurationMs);
    run.runInsights.evidence.put("totalExecutorRunTimeMs", totalExecutorRun);
    run.runInsights.evidence.put("totalGcTimeMs", totalGc);
    run.runInsights.evidence.put("totalSpillBytes", totalSpill);

    if (run.utilization != null) {
      run.runInsights.evidence.put("parallelismUtil", run.utilization.parallelismUtil);
      run.runInsights.evidence.put("classification", run.utilization.classification);
      run.runInsights.evidence.putAll(run.utilization.evidence);
    }

    // --- Recommendations: cluster/workload ---
    if (run.utilization != null) {
      boolean overheadBound =
          (run.durationMs != null && run.durationMs < 120_000) &&
          (totalTasks < 200 || run.runInsights.medianStageDurationMs < 200) &&
          (run.utilization.parallelismUtil < 0.25);

      if (overheadBound) {
        add(run, "Job appears overhead-bound (too small for distributed compute): many tiny stages and low task count. For validation runs, scale input or run in local mode without a cluster.");
        add(run, "If this is a real workload path, reduce the number of Spark actions (extra counts/checkpoints) and combine rules where possible.");
      }

      if ("Under-utilized".equalsIgnoreCase(run.utilization.classification)) {
        add(run, "Cluster under-utilized: consider lowering executor count / dynamic allocation max, or increasing parallelism only if the workload is actually large.");
        if (totalTasks < 4L * safeSlots(run)) {
          add(run, "Parallelism too low relative to available slots: increase input partitions or tune spark.sql.shuffle.partitions for wide ops (groupBy/joins).");
        }
      }

      double gcPct = (totalExecutorRun > 0) ? ((double) totalGc / (double) totalExecutorRun) : 0.0;
      double spillGB = totalSpill / 1_000_000_000.0;

      if (gcPct > 0.20) add(run, "High GC%: reduce object churn (avoid UDF-heavy paths), consider larger executor memory, review join strategy & AQE settings.");
      if (spillGB > 10.0) add(run, "Significant spill detected: review aggregation/join memory usage, skew, and shuffle partitions. Consider salting skewed keys for recon groupBy.");

      Optional<CompactParsedLog.StageSummary> worstSkew = run.stages.stream()
          .max(Comparator.comparingDouble(s -> s.maxOverP50));

      if (worstSkew.isPresent() && worstSkew.get().maxOverP50 > 6.0) {
        var s = worstSkew.get();
        add(run, "Skew detected (maxOverP50=" + round2(s.maxOverP50) + ") in stage " + s.stageId +
            ": enable AQE skew join handling and/or salt skewed keys for grouping by match rule / recon identifiers.");
      }

      for (int i = 0; i < Math.min(3, byExec.size()); i++) {
        var s = byExec.get(i);
        if (s.executorRunTimeMs <= 0) break;
        add(run, "Top stage by executor time: stage " + s.stageId + " (" + safe(s.name) + "), executorRunTimeMs=" + s.executorRunTimeMs +
            ", shuffleRead=" + s.shuffleReadBytes + ", shuffleWrite=" + s.shuffleWriteBytes + ", spill=" + (s.spillMemBytes + s.spillDiskBytes));
      }
    }

    // --- Recommendations: rule domain ---
    if (run.matchRules != null && !run.matchRules.isEmpty()) {
      var rules = new ArrayList<>(run.matchRules);
      rules.sort((a, b) -> Double.compare(
          b.matchTimeSec != null ? b.matchTimeSec : 0.0,
          a.matchTimeSec != null ? a.matchTimeSec : 0.0
      ));

      for (int i = 0; i < Math.min(3, rules.size()); i++) {
        var r = rules.get(i);
        if (r.matchTimeSec == null) continue;
        add(run, "Slow rule: [" + r.rule + "] matchTimeSec=" + r.matchTimeSec +
            ", candidates=" + nz(r.matchCandidates) + ", matches=" + nz(r.matches) +
            ", match%=" + (r.totalMatchPct != null ? r.totalMatchPct : null) +
            ", unmatched=" + nz(r.totalUnmatched));
      }

      for (var r : rules) {
        if (r.totalMatchPct != null && r.totalMatchPct < 5.0 && (r.matchCandidates != null && r.matchCandidates > 100_000)) {
          add(run, "Low match% with high candidates: [" + r.rule + "] match%=" + r.totalMatchPct +
              "%. Consider narrowing scope filters or ordering rules to avoid expensive low-yield rules early.");
          break;
        }
      }

      if (run.ruleStageCorrelations != null && !run.ruleStageCorrelations.isEmpty()) {
        var top = run.ruleStageCorrelations.get(0);
        if (!top.overlaps.isEmpty()) {
          var o = top.overlaps.get(0);
          add(run, "Rule-stage correlation: rule [" + top.rule + "] overlaps most with stage " +
              o.stageId + " (" + safe(o.stageName) + ") overlapMs=" + o.overlapMs +
              " (" + round2(o.overlapPctOfRule * 100) + "% of rule time).");
        }
      }
    }
  }

  private static void add(CompactParsedLog run, String rec) {
    run.runInsights.recommendations.add(rec);
    if (run.utilization != null) run.utilization.recommendations.add(rec);
  }

  private static double median(List<Long> v) {
    if (v == null || v.isEmpty()) return 0.0;
    v.sort(Long::compare);
    int n = v.size();
    if (n % 2 == 1) return v.get(n / 2);
    return (v.get(n / 2 - 1) + v.get(n / 2)) / 2.0;
  }

  private static long safeSlots(CompactParsedLog run) {
    if (run.executors == null) return 1;
    double avgExec = (run.executors.avgExecutors != null && run.executors.avgExecutors > 0)
        ? run.executors.avgExecutors
        : Math.max(1, run.executors.maxExecutors);
    int cores = (run.executors.executorCores != null && run.executors.executorCores > 0) ? run.executors.executorCores : 1;
    return Math.max(1L, (long) Math.ceil(avgExec * cores));
  }

  private static String safe(String s) { return s == null ? "" : s; }
  private static Long nz(Long x) { return x == null ? 0L : x; }
  private static double round2(double x) { return Math.round(x * 100.0) / 100.0; }
}
