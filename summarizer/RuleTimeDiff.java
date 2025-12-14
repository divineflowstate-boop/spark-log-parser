package com.yourorg.sparklog;

public class RuleTimeDiff {
  public String rule;

  public Double baselineTimeSec;
  public Double candidateTimeSec;

  public Double absDeltaSec;
  public Double pctDelta;

  public String classification; // REGRESSED / IMPROVED / UNCHANGED
}
