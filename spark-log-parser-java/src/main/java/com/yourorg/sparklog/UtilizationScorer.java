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

    double util = execMs > 0 ? Math.min(1.0, execMs / (double) appMs) : 0;
    u.parallelismUtil = util;
    u.utilizationScore = (int) (util * 100);

    if (util < 0.45) u.classification = "Under-utilized";
    else if (util > 0.9) u.classification = "Over-utilized";
    else u.classification = "Balanced";

    u.evidence.put("appDurationMs", appMs);
    u.evidence.put("executorRunTimeMs", execMs);

    return u;
  }
}
