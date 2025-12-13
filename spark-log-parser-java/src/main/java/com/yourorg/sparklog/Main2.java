package com.yourorg.sparklog;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.List;

public class Main {
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: java -jar spark-log-parser-java.jar <eventLogPath> <outDir> [driverLogPath] [tz]");
      System.err.println("  tz example: UTC or Asia/Kolkata (default UTC)");
      System.exit(1);
    }

    Path eventLog = Path.of(args[0]);
    File outDir = new File(args[1]);
    outDir.mkdirs();

    ZoneId zone = ZoneId.of(args.length >= 4 ? args[3] : "UTC");

    SparkEventLogParser parser = new SparkEventLogParser();
    CompactParsedLog parsed = parser.parse(eventLog);

    // Optional driver log for MatchingEngine rule lines
    if (args.length >= 3) {
      Path driverLog = Path.of(args[2]);
      List<MatchRuleMetric> rules = MatchingEngineLogParser.parse(driverLog, zone);
      parsed.matchRules.addAll(rules);

      parsed.ruleStageCorrelations = RuleStageCorrelator.correlate(parsed);
    }

    RecommendationsEngine.enrich(parsed);

    ObjectMapper om = new ObjectMapper();
    File parsedFile = new File(outDir, "parsed.json");
    File utilFile = new File(outDir, "utilization.json");

    om.writerWithDefaultPrettyPrinter().writeValue(parsedFile, parsed);
    om.writerWithDefaultPrettyPrinter().writeValue(utilFile, parsed.utilization);

    System.out.println("Wrote: " + parsedFile.getAbsolutePath());
    System.out.println("Wrote: " + utilFile.getAbsolutePath());
    System.out.println("Parsed matchRules=" + parsed.matchRules.size());
    System.out.println("Correlations=" + parsed.ruleStageCorrelations.size());
    System.out.println("Recommendations=" + parsed.runInsights.recommendations.size());
  }
}
