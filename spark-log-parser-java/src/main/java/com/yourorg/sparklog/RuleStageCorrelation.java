package com.yourorg.sparklog;

import java.util.*;

public class RuleStageCorrelation {
  public String rule;
  public Long ruleEndTimeMs;
  public Long ruleStartTimeMs;
  public Double ruleMatchTimeSec;

  public List<Overlap> overlaps = new ArrayList<>();

  public static class Overlap {
    public int stageId;
    public int attemptId;
    public String stageName;

    public Long stageStartMs;
    public Long stageEndMs;

    public long overlapMs;
    public double overlapPctOfRule;
  }
}
