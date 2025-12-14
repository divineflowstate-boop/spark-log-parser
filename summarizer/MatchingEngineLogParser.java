package com.yourorg.sparklog;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses your custom "MatchingEngine" log lines.
 * Assumes each line contains the full rule summary (as you shared).
 *
 * We don't rely on timestamps for correlation (you said "forget correlation for now").
 */
public class MatchingEngineLogParser {

  private static final Pattern RULE = Pattern.compile("Rule \\[(.+?)]");
  private static final Pattern LONG = Pattern.compile("(Match Candidates|Matches|Matches from ageing breaks|Total Unmatched) = (\\d+)");
  private static final Pattern PCT  = Pattern.compile("Total Match % = ([0-9.]+)%");
  private static final Pattern TIME = Pattern.compile("MatchTime = ([0-9.]+)s");

  public static List<MatchRuleMetric> parse(Path logFile, ZoneId zone) throws IOException {
    List<MatchRuleMetric> out = new ArrayList<>();
    for (String line : Files.readAllLines(logFile)) {
      Matcher r = RULE.matcher(line);
      if (!r.find()) continue;

      MatchRuleMetric m = new MatchRuleMetric();
      m.rule = r.group(1);

      Matcher lm = LONG.matcher(line);
      while (lm.find()) {
        long v = Long.parseLong(lm.group(2));
        switch (lm.group(1)) {
          case "Match Candidates" -> m.matchCandidates = v;
          case "Matches" -> m.matches = v;
          case "Matches from ageing breaks" -> m.matchesFromAgeingBreaks = v;
          case "Total Unmatched" -> m.totalUnmatched = v;
        }
      }

      Matcher pm = PCT.matcher(line);
      if (pm.find()) m.totalMatchPct = Double.parseDouble(pm.group(1));

      Matcher tm = TIME.matcher(line);
      if (tm.find()) m.matchTimeSec = Double.parseDouble(tm.group(1));

      out.add(m);
    }
    return out;
  }
}
