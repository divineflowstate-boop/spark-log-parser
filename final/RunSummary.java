
package com.yourorg.sparklog;

import java.util.*;

public class RunSummary {
  public String runId;
  public long durationMs;

  public int totalStages;
  public int totalTasks;

  public double spillGB;
  public double gcOverExecPct;

  public double parallelismUtil;
  public String utilizationClass;

  public List<StageHotspot> topStagesByExecutorTime = new ArrayList<>();

  public static class StageHotspot {
    public int stageId;
    public long executorRunTime;
  }
}
