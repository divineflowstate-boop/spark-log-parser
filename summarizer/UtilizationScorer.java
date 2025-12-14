package com.yourorg.sparklog;

/**
 * Deterministic utilization scoring based on task count and stage/task shape.
 * This is intentionally simple and stable; you can evolve it later using executor metrics.
 */
public class UtilizationScorer {

  public static UtilizationScore score(int totalTasks, double avgInputPartitionMB, double avgShufflePartitionMB) {
    UtilizationScore u = new UtilizationScore();

    // A simple stable proxy:
    // - more tasks => more parallel work
    // - very small partitions => overhead-bound => lower "effective utilization"
    double base = totalTasks <= 0 ? 0.0 : Math.min(1.0, totalTasks / 2000.0);

    double penalty = 0.0;
    if (avgInputPartitionMB > 0 && avgInputPartitionMB < 16) penalty += 0.15;
    if (avgShufflePartitionMB > 0 && avgShufflePartitionMB < 16) penalty += 0.20;

    double effective = Math.max(0.0, base - penalty);

    u.score0to100 = effective * 100.0;

    if (u.score0to100 < 40) {
      u.classification = "UNDER_UTILIZED";
      u.rationale = "Low effective parallel work relative to overhead (tiny partitions and/or low task volume).";
    } else if (u.score0to100 > 90) {
      u.classification = "OVER_UTILIZED";
      u.rationale = "High parallel work; watch for spill/GC/skew.";
    } else {
      u.classification = "OK";
      u.rationale = "Reasonable effective utilization.";
    }

    return u;
  }
}
