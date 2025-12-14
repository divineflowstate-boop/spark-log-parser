package com.yourorg.sparklog;

public class MatchQualityDiff {
  public Double baselineMatchPct;
  public Double candidateMatchPct;

  public Double absDeltaPct; // candidate - baseline
  public Double pctDelta;    // relative to baseline

  public String classification; // IMPROVED / REGRESSED / UNCHANGED / NOT_AVAILABLE
}
