package com.yourorg.sparklog;

import java.util.*;
import static java.util.stream.Collectors.toList;

public class RunSummarizer {

  public static RunSummary summarize(CompactParsedLog log) {
    RunSummary s = new RunSummary();
    s.runId = log.appId;
    s.durationMs = (log.appEndMs > 0 && log.appStartMs > 0) ? (log.appEndMs - log.appStartMs) : 0L;

    s.totalStages = log.stages.size();

    long totalExec = 0, totalGc = 0, totalSpill = 0;
    long totalInputBytes = 0;
    long totalShuffleWriteBytes = 0;

    int totalTasks = 0;
    int inputTasks = 0;
    int shuffleTasks = 0;

    for (var st : log.stages) {
      totalExec += st.executorRunTimeMs;
      totalGc += st.gcTimeMs;
      totalSpill += st.spillBytes;

      totalTasks += st.numTasks;

      if (st.inputBytes > 0) {
        totalInputBytes += st.inputBytes;
        inputTasks += st.numTasks;
      }
      if (st.shuffleWriteBytes > 0) {
        totalShuffleWriteBytes += st.shuffleWriteBytes;
        shuffleTasks += st.numTasks;
      }

      RunSummary.StageHotspot h = new RunSummary.StageHotspot();
      h.stageId = st.stageId;
      h.executorRunTimeMs = st.executorRunTimeMs;
      h.shuffleWriteBytes = st.shuffleWriteBytes;
      h.inputBytes = st.inputBytes;
      h.numTasks = st.numTasks;
      s.topStagesByExecutorTime.add(h);
    }

    s.totalTasks = totalTasks;
    s.spillGB = totalSpill / 1e9;
    s.gcOverExecPct = totalExec > 0 ? (100.0 * totalGc / (double) totalExec) : 0.0;

    // Phase-1 derived sizes
    s.avgInputPartitionMB = (inputTasks > 0) ? ((totalInputBytes / 1e6) / inputTasks) : 0.0;
    s.avgShufflePartitionMB = (shuffleTasks > 0) ? ((totalShuffleWriteBytes / 1e6) / shuffleTasks) : 0.0;

    // Basic util proxy for now (kept stable)
    s.parallelismUtil = totalTasks > 0 ? Math.min(1.0, totalTasks / 2000.0) : 0.0;

    // Deterministic signals: input partition sizing
    if (s.avgInputPartitionMB > 0 && s.avgInputPartitionMB < 32) s.signals.add("INPUT_OVER_PARTITIONED");
    if (s.avgInputPartitionMB > 512) s.signals.add("INPUT_UNDER_PARTITIONED");

    // Deterministic signals: shuffle partition sizing
    if (s.avgShufflePartitionMB > 0 && s.avgShufflePartitionMB < 32) s.signals.add("SHUFFLE_PARTITIONS_TOO_HIGH");
    if (s.avgShufflePartitionMB > 512) s.signals.add("SHUFFLE_PARTITIONS_TOO_LOW");

    if (s.spillGB > 1.0) s.signals.add("SPILL_DETECTED");
    if (s.parallelismUtil < 0.4) s.signals.add("CLUSTER_UNDER_UTILIZED");

    // Phase-2 multiplier suggestions (safe, non-magic)
    s.inputPartitionSuggestion = suggestInputMultiplier(s.avgInputPartitionMB);
    s.shufflePartitionSuggestion = suggestShuffleMultiplier(s.avgShufflePartitionMB);

    // Utilization score (deterministic)
    s.utilizationScore = UtilizationScorer.score(s.totalTasks, s.avgInputPartitionMB, s.avgShufflePartitionMB);
    s.utilizationClass = s.utilizationScore.classification;

    // top stages by executor time
    s.topStagesByExecutorTime.sort((a, b) -> Long.compare(b.executorRunTimeMs, a.executorRunTimeMs));
    if (s.topStagesByExecutorTime.size() > 10) s.topStagesByExecutorTime = s.topStagesByExecutorTime.subList(0, 10);

    // rule hotspots (from driver log)
    if (log.matchRules != null) {
      for (MatchRuleMetric mr : log.matchRules) {
        RunSummary.RuleHotspot rh = new RunSummary.RuleHotspot();
        rh.rule = mr.rule;
        rh.matchCandidates = mr.matchCandidates;
        rh.matches = mr.matches;
        rh.matchesFromAgeingBreaks = mr.matchesFromAgeingBreaks;
        rh.totalUnmatched = mr.totalUnmatched;
        rh.totalMatchPct = mr.totalMatchPct;
        rh.matchTimeSec = mr.matchTimeSec;
        s.topRulesByTime.add(rh);
      }
      s.topRulesByTime.sort((a, b) -> Double.compare(
          b.matchTimeSec != null ? b.matchTimeSec : 0.0,
          a.matchTimeSec != null ? a.matchTimeSec : 0.0
      ));
      if (s.topRulesByTime.size() > 50) s.topRulesByTime = s.topRulesByTime.subList(0, 50);
    }

    return s;
  }

  private static String suggestInputMultiplier(double avgMB) {
    if (avgMB <= 0) return "N/A";
    if (avgMB < 16) return "coalesce ~4–8x (too many tiny input partitions)";
    if (avgMB < 32) return "coalesce ~2–4x (input partitions small)";
    if (avgMB <= 256) return "keep as-is (input partition size healthy)";
    if (avgMB <= 512) return "consider repartition ~2x (input partitions large)";
    return "repartition ~2–4x (input partitions very large)";
  }

  private static String suggestShuffleMultiplier(double avgMB) {
    if (avgMB <= 0) return "N/A";
    if (avgMB < 16) return "reduce shuffle partitions ~4–8x (tiny shuffle partitions / overhead)";
    if (avgMB < 32) return "reduce shuffle partitions ~2–4x (shuffle partitions small)";
    if (avgMB <= 256) return "keep as-is (shuffle partition size healthy)";
    if (avgMB <= 512) return "increase shuffle partitions ~2x (large shuffle partitions)";
    return "increase shuffle partitions ~2–4x (very large shuffle partitions / spill risk)";
  }
}
