package com.yourorg.sparklog;

public class UtilizationScorer {

  public static UtilizationScore score(CompactParsedLog p) {
    UtilizationScore u = new UtilizationScore();

    if (p.startTimeMs == null || p.endTimeMs == null) {
      u.classification = "Unknown";
      return u;
    }

    long appMs = p.endTimeMs - p.startTimeMs;
    long execMs = p.stages.stream().mapToLong(s -> s.executorRunTimeMs).sum();

    double avgExecutors =
        (p.executors.avgExecutors != null && p.executors.avgExecutors > 0)
            ? p.executors.avgExecutors
            : Math.max(1, p.executors.maxExecutors);

    int cores =
        (p.executors.executorCores != null && p.executors.executorCores > 0)
            ? p.executors.executorCores
            : 1;

    double slotTimeAvailable = avgExecutors * cores * appMs;

    double util = slotTimeAvailable > 0 ? (execMs / slotTimeAvailable) : 0.0;
    util = Math.min(1.0, util);

    u.parallelismUtil = util;
    u.utilizationScore = (int) Math.round(util * 100);

    if (util < 0.45) u.classification = "Under-utilized";
    else if (util > 0.9) u.classification = "Over-utilized";
    else u.classification = "Balanced";

    u.evidence.put("executorRunTimeMs", execMs);
    u.evidence.put("executorCores", cores);
    u.evidence.put("slotTimeAvailableMs", slotTimeAvailable);
    u.evidence.put("appDurationMs", appMs);
    u.evidence.put("avgExecutors", avgExecutors);

    return u;
  }
}
