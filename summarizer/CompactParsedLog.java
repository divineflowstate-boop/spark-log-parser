package com.yourorg.sparklog;

import java.util.*;

public class CompactParsedLog {

  public String appId;
  public long appStartMs;
  public long appEndMs;

  public java.util.List<Stage> stages = new ArrayList<>();
  public java.util.List<MatchRuleMetric> matchRules = new ArrayList<>();

  public static class Stage {
    public int stageId;
    public long submissionMs;
    public long completionMs;

    public long executorRunTimeMs;
    public long gcTimeMs;

    public long inputBytes;
    public long shuffleReadBytes;
    public long shuffleWriteBytes;
    public long spillBytes;

    public int numTasks;
  }
}
