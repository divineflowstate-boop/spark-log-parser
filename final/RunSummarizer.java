
package com.yourorg.sparklog;

public class RunSummarizer {

  public static RunSummary summarize(CompactParsedLog log) {
    RunSummary s = new RunSummary();
    s.runId = log.appId;
    s.durationMs = log.endTimeMs - log.startTimeMs;

    s.totalStages = log.stages.size();

    long exec = 0, gc = 0, spill = 0;
    int tasks = 0;

    for (var st : log.stages.values()) {
      exec += st.executorRunTime;
      gc += st.gcTime;
      spill += st.spillBytes;
      tasks += st.numTasks;

      RunSummary.StageHotspot h = new RunSummary.StageHotspot();
      h.stageId = st.stageId;
      h.executorRunTime = st.executorRunTime;
      s.topStagesByExecutorTime.add(h);
    }

    s.totalTasks = tasks;
    s.spillGB = spill / 1e9;
    s.gcOverExecPct = exec > 0 ? (100.0 * gc / exec) : 0.0;

    s.parallelismUtil = tasks > 0 ? Math.min(1.0, tasks / 2000.0) : 0.0;
    s.utilizationClass = s.parallelismUtil < 0.3 ? "UNDER_UTILIZED" : "OK";

    s.topStagesByExecutorTime.sort(
        (a, b) -> Long.compare(b.executorRunTime, a.executorRunTime));

    return s;
  }
}
