
package com.yourorg.sparklog;

import java.util.*;

public class CompactParsedLog {
  public String appId;
  public long startTimeMs;
  public long endTimeMs;

  public Map<Integer, StageInfo> stages = new HashMap<>();

  public static class StageInfo {
    public int stageId;
    public long submissionTimeMs;
    public long completionTimeMs;
    public long executorRunTime;
    public long gcTime;
    public long spillBytes;
    public int numTasks;
  }
}
