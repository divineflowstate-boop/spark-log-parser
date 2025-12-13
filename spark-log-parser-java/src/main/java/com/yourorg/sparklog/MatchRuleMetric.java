package com.yourorg.sparklog;

public class MatchRuleMetric {
  public String rule;
  public Long matchCandidates;
  public Long matches;
  public Long matchesFromAgeingBreaks;
  public Double totalMatchPct;
  public Long totalUnmatched;
  public Double matchTimeSec;

  public Long timestampMs; // from log prefix (end time of rule line)
  public Integer jobId;    // reserved if you later parse Job id
}
