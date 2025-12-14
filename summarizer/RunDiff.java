package com.yourorg.sparklog;

import java.util.*;

public class RunDiff {

  public RunSummary baseline;
  public RunSummary candidate;

  // compact numeric deltas for easy dashboards
  public Map<String, MetricDelta> deltas = new LinkedHashMap<>();

  public List<String> regressions = new ArrayList<>();
  public List<String> improvements = new ArrayList<>();

  // Matching time comparison (sum of rule times)
  public Double baselineTotalMatchingTimeSec;
  public Double candidateTotalMatchingTimeSec;
  public Double matchingTimePctChange;

  // Rule presence/time diffs
  public List<String> rulesOnlyInBaseline = new ArrayList<>();
  public List<String> rulesOnlyInCandidate = new ArrayList<>();
  public List<RuleTimeDiff> ruleTimeDiffs = new ArrayList<>();

  // Match quality and diffs
  public MatchQualityDiff overallMatchQuality;
  public List<RuleMatchPctDiff> ruleMatchPctDiffs = new ArrayList<>();

  // Contribution + precision/recall
  public List<RuleMatchContribution> ruleMatchContributions = new ArrayList<>();
  public PrecisionRecallDiff precisionRecall;

  // Phase-1/2 deltas (input/shuffle sizing)
  public String inputPartitionSuggestionBaseline;
  public String inputPartitionSuggestionCandidate;
  public String shufflePartitionSuggestionBaseline;
  public String shufflePartitionSuggestionCandidate;

  public static class MetricDelta {
    public Double baseline;
    public Double candidate;
    public Double absDelta;
    public Double pctDelta;
  }
}
