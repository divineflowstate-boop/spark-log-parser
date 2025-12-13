package com.yourorg.sparklog;

import java.util.*;

public class RuleStageCorrelator {

  public static List<RuleStageCorrelation> correlate(CompactParsedLog run) {
    if (run.matchRules == null || run.matchRules.isEmpty()) return List.of();
    if (run.stages == null || run.stages.isEmpty()) return List.of();

    List<RuleStageCorrelation> out = new ArrayList<>();

    for (MatchRuleMetric r : run.matchRules) {
      if (r.timestampMs == null || r.matchTimeSec == null) continue;

      long end = r.timestampMs;
      long start = end - Math.max(1L, (long) (r.matchTimeSec * 1000.0));

      RuleStageCorrelation c = new RuleStageCorrelation();
      c.rule = r.rule;
      c.ruleEndTimeMs = end;
      c.ruleStartTimeMs = start;
      c.ruleMatchTimeSec = r.matchTimeSec;

      long ruleDur = Math.max(1L, end - start);

      for (CompactParsedLog.StageSummary s : run.stages) {
        if (s.submissionTimeMs == null || s.completionTimeMs == null) continue;

        long ss = s.submissionTimeMs;
        long se = s.completionTimeMs;
        if (se <= ss) continue;

        long overlap = overlapMs(start, end, ss, se);
        if (overlap <= 0) continue;

        RuleStageCorrelation.Overlap o = new RuleStageCorrelation.Overlap();
        o.stageId = s.stageId;
        o.attemptId = s.attemptId;
        o.stageName = s.name;

        o.stageStartMs = ss;
        o.stageEndMs = se;

        o.overlapMs = overlap;
        o.overlapPctOfRule = (double) overlap / (double) ruleDur;

        c.overlaps.add(o);
      }

      c.overlaps.sort((a, b) -> Long.compare(b.overlapMs, a.overlapMs));
      out.add(c);
    }

    out.sort((a, b) -> Long.compare(
        (b.ruleEndTimeMs - b.ruleStartTimeMs),
        (a.ruleEndTimeMs - a.ruleStartTimeMs)
    ));
    return out;
  }

  private static long overlapMs(long aStart, long aEnd, long bStart, long bEnd) {
    long s = Math.max(aStart, bStart);
    long e = Math.min(aEnd, bEnd);
    return Math.max(0L, e - s);
  }
}
