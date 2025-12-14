package com.yourorg.sparklog;

public class PrecisionRecallDiff {
  public Long baselineMatches;
  public Long candidateMatches;

  public Long baselineUnmatched;
  public Long candidateUnmatched;

  public Double baselineMatchPct;
  public Double candidateMatchPct;

  public String classification;
  /*
    PRECISION_UP_RECALL_DOWN
    PRECISION_DOWN_RECALL_UP
    BOTH_UP
    BOTH_DOWN
    NO_SIGNIFICANT_CHANGE
    NOT_AVAILABLE
  */
}
