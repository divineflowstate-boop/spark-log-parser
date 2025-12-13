package com.yourorg.sparklog;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.*;

public class MatchingEngineLogParser {

  private static final Pattern RULE_PATTERN =
      Pattern.compile("Rule\\s*\\[(?<rule>[^\\]]+)]");

  private static final Pattern LONG_FIELD =
      Pattern.compile("(?<key>Match Candidates|Matches from ageing breaks|Matches|Total Unmatched)\\s*=\\s*(?<val>\\d+)");

  private static final Pattern PCT_FIELD =
      Pattern.compile("Total Match %\\s*=\\s*(?<val>[0-9.]+)%");

  private static final Pattern TIME_FIELD =
      Pattern.compile("MatchTime\\s*=\\s*(?<val>[0-9.]+)s");

  private static final Pattern TS_PREFIX =
      Pattern.compile("^(?<dt>\\d{2}/\\d{2}/\\d{2}\\s+\\d{2}:\\d{2}:\\d{2})\\s+");

  private static final DateTimeFormatter TS_FMT =
      DateTimeFormatter.ofPattern("dd/MM/yy HH:mm:ss");

  public static List<MatchRuleMetric> parse(Path logPath, ZoneId zone) throws IOException {
    List<MatchRuleMetric> out = new ArrayList<>();

    try (BufferedReader br = Files.newBufferedReader(logPath, StandardCharsets.UTF_8)) {
      String line;
      while ((line = br.readLine()) != null) {

        Matcher ruleMatcher = RULE_PATTERN.matcher(line);
        if (!ruleMatcher.find()) continue;

        MatchRuleMetric r = new MatchRuleMetric();
        r.rule = ruleMatcher.group("rule");
        r.timestampMs = parseTimestampMs(line, zone);

        extractLongs(line, r);
        extractPct(line, r);
        extractTime(line, r);

        out.add(r);
      }
    }

    return out;
  }

  private static Long parseTimestampMs(String line, ZoneId zone) {
    Matcher m = TS_PREFIX.matcher(line);
    if (!m.find()) return null;

    String dt = m.group("dt");
    try {
      LocalDateTime ldt = LocalDateTime.parse(dt, TS_FMT);
      return ldt.atZone(zone).toInstant().toEpochMilli();
    } catch (Exception ignored) {
      return null;
    }
  }

  private static void extractLongs(String line, MatchRuleMetric r) {
    Matcher m = LONG_FIELD.matcher(line);
    while (m.find()) {
      long v = Long.parseLong(m.group("val"));
      switch (m.group("key")) {
        case "Match Candidates" -> r.matchCandidates = v;
        case "Matches" -> r.matches = v;
        case "Matches from ageing breaks" -> r.matchesFromAgeingBreaks = v;
        case "Total Unmatched" -> r.totalUnmatched = v;
      }
    }
  }

  private static void extractPct(String line, MatchRuleMetric r) {
    Matcher m = PCT_FIELD.matcher(line);
    if (m.find()) r.totalMatchPct = Double.parseDouble(m.group("val"));
  }

  private static void extractTime(String line, MatchRuleMetric r) {
    Matcher m = TIME_FIELD.matcher(line);
    if (m.find()) r.matchTimeSec = Double.parseDouble(m.group("val"));
  }
}
