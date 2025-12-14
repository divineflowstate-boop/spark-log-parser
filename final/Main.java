
package com.yourorg.sparklog;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.nio.file.Path;

public class Main {
  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: summarize|diff");
      System.exit(1);
    }

    if ("summarize".equals(args[0])) {
      Path eventLog = Path.of(args[1]);
      File out = new File(args[2]);
      out.mkdirs();

      CompactParsedLog parsed = new SparkEventLogParser().parse(eventLog);
      RunSummary summary = RunSummarizer.summarize(parsed);

      ObjectMapper om = new ObjectMapper();
      om.writerWithDefaultPrettyPrinter().writeValue(new File(out, "summary.json"), summary);

    } else if ("diff".equals(args[0])) {
      Path a = Path.of(args[1]);
      Path b = Path.of(args[2]);
      File out = new File(args[3]);
      out.mkdirs();

      RunSummary sa = RunSummarizer.summarize(new SparkEventLogParser().parse(a));
      RunSummary sb = RunSummarizer.summarize(new SparkEventLogParser().parse(b));

      RunDiff diff = RunDiffer.diff(sa, sb);

      ObjectMapper om = new ObjectMapper();
      om.writerWithDefaultPrettyPrinter().writeValue(new File(out, "diff.json"), diff);
      write(new File(out, "llm_prompt.txt").toPath(), PromptBuilder.buildPrompt(diff));
    }
  }
}
