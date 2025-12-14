package com.yourorg.sparklog;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.nio.file.Path;
import java.time.ZoneId;

public class Main {

  /**
   * summarize <eventLog> <outDir> [driverLog] [tz]
   * diff <eventA> <eventB> <outDir> [driverA] [driverB] [tz]
   */
  public static void main(String[] args) throws Exception {
    if (args.length < 1) usageAndExit();

    String mode = args[0].toLowerCase();
    ObjectMapper om = new ObjectMapper();

    if ("summarize".equals(mode)) {
      if (args.length < 3) usageAndExit();
      Path eventLog = Path.of(args[1]);
      File outDir = new File(args[2]);
      outDir.mkdirs();

      ZoneId zone = ZoneId.of(args.length >= 5 ? args[4] : "Asia/Kolkata");

      CompactParsedLog parsed = new SparkEventLogParser().parse(eventLog);
      if (args.length >= 4) {
        parsed.matchRules.addAll(MatchingEngineLogParser.parse(Path.of(args[3]), zone));
      }

      RunSummary summary = RunSummarizer.summarize(parsed);
      om.writerWithDefaultPrettyPrinter().writeValue(new File(outDir, "parsed.json"), parsed);
      om.writerWithDefaultPrettyPrinter().writeValue(new File(outDir, "summary.json"), summary);
      System.out.println("Wrote parsed.json + summary.json to " + outDir.getAbsolutePath());
      return;
    }

    if ("diff".equals(mode)) {
      if (args.length < 4) usageAndExit();

      Path a = Path.of(args[1]);
      Path b = Path.of(args[2]);
      File outDir = new File(args[3]);
      outDir.mkdirs();

      ZoneId zone = ZoneId.of(args.length >= 7 ? args[6] : "Asia/Kolkata");

      CompactParsedLog pa = new SparkEventLogParser().parse(a);
      CompactParsedLog pb = new SparkEventLogParser().parse(b);

      if (args.length >= 5) pa.matchRules.addAll(MatchingEngineLogParser.parse(Path.of(args[4]), zone));
      if (args.length >= 6) pb.matchRules.addAll(MatchingEngineLogParser.parse(Path.of(args[5]), zone));

      RunSummary sa = RunSummarizer.summarize(pa);
      RunSummary sb = RunSummarizer.summarize(pb);

      RunDiff diff = RunDiffer.diff(sa, sb);
      String prompt = PromptBuilder.buildPrompt(diff);

      om.writerWithDefaultPrettyPrinter().writeValue(new File(outDir, "baseline_summary.json"), sa);
      om.writerWithDefaultPrettyPrinter().writeValue(new File(outDir, "candidate_summary.json"), sb);
      om.writerWithDefaultPrettyPrinter().writeValue(new File(outDir, "diff.json"), diff);
      java.nio.file.Files.writeString(new File(outDir, "llm_prompt.txt").toPath(), prompt);

      System.out.println("Wrote baseline/candidate summaries, diff.json, llm_prompt.txt to " + outDir.getAbsolutePath());
      return;
    }

    usageAndExit();
  }

  private static void usageAndExit() {
    System.err.println("""
      Usage:
        summarize <eventLog> <outDir> [driverLog] [tz]
        diff <eventA> <eventB> <outDir> [driverA] [driverB] [tz]

      Notes:
        - Spark event logs are JSON lines (optionally .gz)
        - driverLog is your custom MatchingEngine log file (optional)
        - tz default: Asia/Kolkata
    """);
    System.exit(1);
  }
}
