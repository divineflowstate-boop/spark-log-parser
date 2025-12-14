
package com.yourorg.sparklog;

public class RunDiffer {

  public static RunDiff diff(RunSummary a, RunSummary b) {
    RunDiff d = new RunDiff();
    d.baseline = a;
    d.candidate = b;

    d.deltas.put("durationPct",
        a.durationMs > 0 ? 100.0 * (b.durationMs - a.durationMs) / a.durationMs : null);
    d.deltas.put("spillGB", b.spillGB - a.spillGB);
    d.deltas.put("parallelismUtil", b.parallelismUtil - a.parallelismUtil);

    return d;
  }
}
