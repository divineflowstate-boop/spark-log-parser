package com.yourorg.sparklog;

public class RuleMatchContribution {
  public String rule;

  public Long baselineCandidates;
  public Long candidateCandidates;

  public Long baselineMatches;
  public Long candidateMatches;

  public Double baselineMatchPct;
  public Double candidateMatchPct;

  // contribution to overall match delta in percentage points
  public Double contributionPctPoints;

  public String impact; // MAJOR_POSITIVE / MINOR_POSITIVE / NEUTRAL / MINOR_NEGATIVE / MAJOR_NEGATIVE
}
