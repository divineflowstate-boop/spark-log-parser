package com.yourorg.sparklog;

import java.util.*;

public class RunDiffer {

  public static RunDiff diff(RunSummary base, RunSummary cand) {
    RunDiff d = new RunDiff();
    d.baseline = base;
    d.candidate = cand;

    put(d, "durationMs", toD(base.durationMs), toD(cand.durationMs));
    put(d, "spillGB", base.spillGB, cand.spillGB);
    put(d, "gcOverExecPct", base.gcOverExecPct, cand.gcOverExecPct);
    put(d, "avgInputPartitionMB", base.avgInputPartitionMB, cand.avgInputPartitionMB);
    put(d, "avgShufflePartitionMB", base.avgShufflePartitionMB, cand.avgShufflePartitionMB);
    put(d, "utilScore", base.utilizationScore != null ? base.utilizationScore.score0to100 : null,
                   cand.utilizationScore != null ? cand.utilizationScore.score0to100 : null);

    // include Phase-2 suggestions (string) in diff
    d.inputPartitionSuggestionBaseline = base.inputPartitionSuggestion;
    d.inputPartitionSuggestionCandidate = cand.inputPartitionSuggestion;
    d.shufflePartitionSuggestionBaseline = base.shufflePartitionSuggestion;
    d.shufflePartitionSuggestionCandidate = cand.shufflePartitionSuggestion;

    tag(d);

    // matching time
    computeMatchingTimeDiff(d, base, cand);

    // rule presence + per rule time diff
    computeRuleTimeDiffs(d, base, cand);

    // overall match quality
    computeOverallMatchQualityDiff(d, base, cand);

    // per-rule match pct
    computeRuleMatchPctDiffs(d, base, cand);

    // rule contribution + precision/recall
    computeRuleMatchContributions(d, base, cand);
    computePrecisionRecallDiff(d, base, cand);

    return d;
  }

  private static void put(RunDiff d, String name, Double b, Double c) {
    RunDiff.MetricDelta md = new RunDiff.MetricDelta();
    md.baseline = b;
    md.candidate = c;
    if (b != null && c != null) {
      md.absDelta = c - b;
      md.pctDelta = (Math.abs(b) > 1e-9) ? ((c - b) / b) * 100.0 : null;
    }
    d.deltas.put(name, md);
  }

  private static void tag(RunDiff d) {
    var dur = d.deltas.get("durationMs");
    if (dur != null && dur.pctDelta != null && dur.absDelta != null) {
      if (dur.pctDelta > 20.0 && dur.absDelta > 5000) d.regressions.add("DURATION_REGRESSED");
      if (dur.pctDelta < -20.0 && dur.absDelta < -5000) d.improvements.add("DURATION_IMPROVED");
    }
    var spill = d.deltas.get("spillGB");
    if (spill != null && spill.absDelta != null) {
      if (spill.absDelta > 2.0) d.regressions.add("SPILL_INCREASED");
      if (spill.absDelta < -2.0) d.improvements.add("SPILL_REDUCED");
    }
    var util = d.deltas.get("utilScore");
    if (util != null && util.absDelta != null) {
      if (util.absDelta < -15.0) d.regressions.add("UTILIZATION_DROPPED");
      if (util.absDelta > 15.0) d.improvements.add("UTILIZATION_IMPROVED");
    }
  }

  // ----- matching time -----
  private static void computeMatchingTimeDiff(RunDiff d, RunSummary base, RunSummary cand) {
    double b = sumRuleTime(base);
    double c = sumRuleTime(cand);
    d.baselineTotalMatchingTimeSec = b;
    d.candidateTotalMatchingTimeSec = c;
    d.matchingTimePctChange = b > 0 ? ((c - b) / b) * 100.0 : null;
  }

  private static double sumRuleTime(RunSummary s) {
    if (s.topRulesByTime == null) return 0.0;
    double sum = 0.0;
    for (var r : s.topRulesByTime) if (r.matchTimeSec != null) sum += r.matchTimeSec;
    return sum;
  }

  // ----- rule time diffs -----
  private static void computeRuleTimeDiffs(RunDiff d, RunSummary base, RunSummary cand) {
    Map<String, Double> bm = ruleTimeMap(base);
    Map<String, Double> cm = ruleTimeMap(cand);

    for (String r : bm.keySet()) if (!cm.containsKey(r)) d.rulesOnlyInBaseline.add(r);
    for (String r : cm.keySet()) if (!bm.containsKey(r)) d.rulesOnlyInCandidate.add(r);

    for (String rule : bm.keySet()) {
      if (!cm.containsKey(rule)) continue;
      Double b = bm.get(rule);
      Double c = cm.get(rule);

      RuleTimeDiff rt = new RuleTimeDiff();
      rt.rule = rule;
      rt.baselineTimeSec = b;
      rt.candidateTimeSec = c;

      if (b != null && c != null) {
        rt.absDeltaSec = c - b;
        rt.pctDelta = (b > 0) ? ((c - b) / b) * 100.0 : null;
        if (rt.pctDelta != null && rt.pctDelta > 25.0) rt.classification = "REGRESSED";
        else if (rt.pctDelta != null && rt.pctDelta < -25.0) rt.classification = "IMPROVED";
        else rt.classification = "UNCHANGED";
      } else {
        rt.classification = "UNCHANGED";
      }

      d.ruleTimeDiffs.add(rt);
    }

    d.rulesOnlyInBaseline.sort(String::compareTo);
    d.rulesOnlyInCandidate.sort(String::compareTo);

    d.ruleTimeDiffs.sort((a, b) -> Double.compare(
        b.absDeltaSec != null ? b.absDeltaSec : 0.0,
        a.absDeltaSec != null ? a.absDeltaSec : 0.0
    ));
  }

  private static Map<String, Double> ruleTimeMap(RunSummary s) {
    Map<String, Double> m = new HashMap<>();
    if (s.topRulesByTime == null) return m;
    for (var r : s.topRulesByTime) {
      if (r.rule != null && r.matchTimeSec != null) m.put(r.rule, r.matchTimeSec);
    }
    return m;
  }

  // ----- overall match quality (weighted by candidates) -----
  private static void computeOverallMatchQualityDiff(RunDiff d, RunSummary base, RunSummary cand) {
    Double bp = overallMatchPct(base);
    Double cp = overallMatchPct(cand);

    MatchQualityDiff mq = new MatchQualityDiff();
    mq.baselineMatchPct = bp;
    mq.candidateMatchPct = cp;

    if (bp != null && cp != null) {
      mq.absDeltaPct = cp - bp;
      mq.pctDelta = (bp > 0) ? ((cp - bp) / bp) * 100.0 : null;
      if (mq.absDeltaPct > 2.0) mq.classification = "IMPROVED";
      else if (mq.absDeltaPct < -2.0) mq.classification = "REGRESSED";
      else mq.classification = "UNCHANGED";
    } else {
      mq.classification = "NOT_AVAILABLE";
    }

    d.overallMatchQuality = mq;
  }

  private static Double overallMatchPct(RunSummary s) {
    if (s.topRulesByTime == null || s.topRulesByTime.isEmpty()) return null;
    long cand = 0, match = 0;
    for (var r : s.topRulesByTime) {
      if (r.matchCandidates != null && r.matches != null) {
        cand += r.matchCandidates;
        match += r.matches;
      }
    }
    if (cand == 0) return null;
    return (100.0 * match) / cand;
  }

  // ----- per-rule match% diffs -----
  private static void computeRuleMatchPctDiffs(RunDiff d, RunSummary base, RunSummary cand) {
    Map<String, Double> bm = ruleMatchPctMap(base);
    Map<String, Double> cm = ruleMatchPctMap(cand);

    for (String rule : bm.keySet()) {
      if (!cm.containsKey(rule)) continue;
      Double b = bm.get(rule);
      Double c = cm.get(rule);

      RuleMatchPctDiff rd = new RuleMatchPctDiff();
      rd.rule = rule;
      rd.baselineMatchPct = b;
      rd.candidateMatchPct = c;

      if (b != null && c != null) {
        rd.absDeltaPct = c - b;
        rd.pctDelta = (b > 0) ? ((c - b) / b) * 100.0 : null;
        if (rd.absDeltaPct > 2.0) rd.classification = "IMPROVED";
        else if (rd.absDeltaPct < -2.0) rd.classification = "REGRESSED";
        else rd.classification = "UNCHANGED";
      } else {
        rd.classification = "UNCHANGED";
      }

      d.ruleMatchPctDiffs.add(rd);
    }

    d.ruleMatchPctDiffs.sort((a, b) -> Double.compare(
        a.absDeltaPct != null ? a.absDeltaPct : 0.0,
        b.absDeltaPct != null ? b.absDeltaPct : 0.0
    ));
  }

  private static Map<String, Double> ruleMatchPctMap(RunSummary s) {
    Map<String, Double> m = new HashMap<>();
    if (s.topRulesByTime == null) return m;
    for (var r : s.topRulesByTime) {
      if (r.rule != null && r.totalMatchPct != null) m.put(r.rule, r.totalMatchPct);
    }
    return m;
  }

  // ----- rule contribution (weighted) -----
  private static void computeRuleMatchContributions(RunDiff d, RunSummary base, RunSummary cand) {
    if (base.topRulesByTime == null || cand.topRulesByTime == null) return;

    Map<String, RunSummary.RuleHotspot> bm = ruleToRuleMap(base);
    Map<String, RunSummary.RuleHotspot> cm = ruleToRuleMap(cand);

    long baseTotalCand = 0;
    for (var r : base.topRulesByTime) if (r.matchCandidates != null) baseTotalCand += r.matchCandidates;
    if (baseTotalCand == 0) return;

    for (String rule : bm.keySet()) {
      if (!cm.containsKey(rule)) continue;
      var b = bm.get(rule);
      var c = cm.get(rule);

      if (b.matchCandidates == null || c.matchCandidates == null) continue;
      if (b.matches == null || c.matches == null) continue;
      if (b.matchCandidates == 0) continue;

      double bPct = 100.0 * b.matches / b.matchCandidates;
      double cPct = (c.matchCandidates > 0) ? (100.0 * c.matches / c.matchCandidates) : 0.0;

      double weight = (double) b.matchCandidates / baseTotalCand;
      double contrib = (cPct - bPct) * weight;

      RuleMatchContribution rc = new RuleMatchContribution();
      rc.rule = rule;

      rc.baselineCandidates = b.matchCandidates;
      rc.candidateCandidates = c.matchCandidates;
      rc.baselineMatches = b.matches;
      rc.candidateMatches = c.matches;
      rc.baselineMatchPct = bPct;
      rc.candidateMatchPct = cPct;

      rc.contributionPctPoints = contrib;

      if (contrib > 2.0) rc.impact = "MAJOR_POSITIVE";
      else if (contrib > 0.5) rc.impact = "MINOR_POSITIVE";
      else if (contrib < -2.0) rc.impact = "MAJOR_NEGATIVE";
      else if (contrib < -0.5) rc.impact = "MINOR_NEGATIVE";
      else rc.impact = "NEUTRAL";

      d.ruleMatchContributions.add(rc);
    }

    d.ruleMatchContributions.sort((a, b) -> Double.compare(
        Math.abs(b.contributionPctPoints != null ? b.contributionPctPoints : 0.0),
        Math.abs(a.contributionPctPoints != null ? a.contributionPctPoints : 0.0)
    ));
  }

  private static Map<String, RunSummary.RuleHotspot> ruleToRuleMap(RunSummary s) {
    Map<String, RunSummary.RuleHotspot> m = new HashMap<>();
    if (s.topRulesByTime == null) return m;
    for (var r : s.topRulesByTime) if (r.rule != null) m.put(r.rule, r);
    return m;
  }

  // ----- precision vs recall signal -----
  private static void computePrecisionRecallDiff(RunDiff d, RunSummary base, RunSummary cand) {
    PrecisionRecallDiff pr = new PrecisionRecallDiff();

    long bMatch = 0, cMatch = 0;
    long bUn = 0, cUn = 0;

    if (base.topRulesByTime != null) {
      for (var r : base.topRulesByTime) {
        if (r.matches != null) bMatch += r.matches;
        if (r.totalUnmatched != null) bUn += r.totalUnmatched;
      }
    }
    if (cand.topRulesByTime != null) {
      for (var r : cand.topRulesByTime) {
        if (r.matches != null) cMatch += r.matches;
        if (r.totalUnmatched != null) cUn += r.totalUnmatched;
      }
    }

    pr.baselineMatches = bMatch;
    pr.candidateMatches = cMatch;
    pr.baselineUnmatched = bUn;
    pr.candidateUnmatched = cUn;

    pr.baselineMatchPct = (bMatch + bUn) > 0 ? (100.0 * bMatch / (double)(bMatch + bUn)) : null;
    pr.candidateMatchPct = (cMatch + cUn) > 0 ? (100.0 * cMatch / (double)(cMatch + cUn)) : null;

    if (pr.baselineMatchPct == null || pr.candidateMatchPct == null) {
      pr.classification = "NOT_AVAILABLE";
      d.precisionRecall = pr;
      return;
    }

    boolean precisionUp = pr.candidateMatchPct > pr.baselineMatchPct + 2.0;
    boolean precisionDown = pr.candidateMatchPct < pr.baselineMatchPct - 2.0;

    boolean recallUp = cMatch > bMatch;
    boolean recallDown = cMatch < bMatch;

    if (precisionUp && recallDown) pr.classification = "PRECISION_UP_RECALL_DOWN";
    else if (precisionDown && recallUp) pr.classification = "PRECISION_DOWN_RECALL_UP";
    else if (precisionUp && recallUp) pr.classification = "BOTH_UP";
    else if (precisionDown && recallDown) pr.classification = "BOTH_DOWN";
    else pr.classification = "NO_SIGNIFICANT_CHANGE";

    d.precisionRecall = pr;
  }

  private static Double toD(long x) { return (double) x; }
}
