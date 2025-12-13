package com.yourorg.sparklog;

import java.util.*;

public class CompactParsedLog {
  public String appId;
  public Long startTimeMs;
  public Long endTimeMs;
  public Long durationMs;
  public String status;

  public Map<String, String> sparkConf = new HashMap<>();
  public ExecutorSummary executors = new ExecutorSummary();
  public List<StageSummary> stages = new ArrayList<>();

  // NEW: driver-log rule metrics + correlation + insights
  public List<MatchRuleMetric> matchRules = new ArrayList<>();
  public List<RuleStageCorrelation> ruleStageCorrelations = new ArrayList<>();
  public RunInsights runInsights = new RunInsights();

  public UtilizationScore utilization;

  public static class ExecutorSummary {
    public int maxExecutors;
    public int totalAdded;
    public int totalRemoved;
    public Integer executorCores;
    public Double avgExecutors;
  }

  public static class StageSummary {
    public int stageId;
    public int attemptId;
    public String name;

    // NEW: needed for correlation
    public Long submissionTimeMs;
    public Long completionTimeMs;

    public Long durationMs;
    public long numTasks;

    public long executorRunTimeMs;
    public long gcTimeMs;

    public long shuffleReadBytes;
    public long shuffleWriteBytes;

    public long spillMemBytes;
    public long spillDiskBytes;

    public long maxTaskDurationMs;
    public double p50TaskDurationMs;
    public double p95TaskDurationMs;
    public double maxOverP50;
    public double stragglerPct;
  }

  // NEW: overall run insights + recs
  public static class RunInsights {
    public long totalTasks;
    public int totalStages;
    public long tinyStages; // duration < 200ms
    public double medianStageDurationMs;
    public List<String> recommendations = new ArrayList<>();
    public Map<String, Object> evidence = new HashMap<>();
  }
}
