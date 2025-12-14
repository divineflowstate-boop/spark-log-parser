package com.yourorg.sparklog;

import java.util.*;

public class RunSummary {

  public String runId;
  public long durationMs;

  public int totalStages;
  public int totalTasks;

  public double spillGB;
  public double gcOverExecPct;

  // Phase-1 metrics
  public double avgInputPartitionMB;
  public double avgShufflePartitionMB;

  // Phase-2 suggestions (multipliers, not magic numbers)
  public String inputPartitionSuggestion;   // e.g., "coalesce ~2-4x"
  public String shufflePartitionSuggestion; // e.g., "reduce shuffle partitions ~4-8x"

  // utilization
  public double parallelismUtil; // 0..1 proxy
  public String utilizationClass;
  public UtilizationScore utilizationScore;

  // deterministic signals
  public java.util.List<String> signals = new ArrayList<>();

  // top stages (helpful context)
  public java.util.List<StageHotspot> topStagesByExecutorTime = new ArrayList<>();

  // top rules by time (from driver log)
  public java.util.List<RuleHotspot> topRulesByTime = new ArrayList<>();

  public static class StageHotspot {
    public int stageId;
    public long executorRunTimeMs;
    public long shuffleWriteBytes;
    public long inputBytes;
    public int numTasks;
  }

  public static class RuleHotspot {
    public String rule;
    public Long matchCandidates;
    public Long matches;
    public Long matchesFromAgeingBreaks;
    public Long totalUnmatched;
    public Double totalMatchPct;
    public Double matchTimeSec;
  }
}
