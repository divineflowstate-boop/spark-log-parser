package com.yourorg.sparklog;

public class RuleMatchPctDiff {
  public String rule;

  public Double baselineMatchPct;
  public Double candidateMatchPct;

  public Double absDeltaPct;
  public Double pctDelta;

  public String classification; // IMPROVED / REGRESSED / UNCHANGED
}
