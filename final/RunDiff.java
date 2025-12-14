
package com.yourorg.sparklog;

import java.util.*;

public class RunDiff {
  public RunSummary baseline;
  public RunSummary candidate;
  public Map<String, Double> deltas = new LinkedHashMap<>();
}
