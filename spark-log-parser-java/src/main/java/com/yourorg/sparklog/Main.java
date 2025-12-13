package com.yourorg.sparklog;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.nio.file.Path;

public class Main {
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: java -jar app.jar <eventlog> <outdir>");
      System.exit(1);
    }

    SparkEventLogParser p = new SparkEventLogParser();
    CompactParsedLog out = p.parse(Path.of(args[0]));

    new File(args[1]).mkdirs();
    ObjectMapper m = new ObjectMapper();
    m.writerWithDefaultPrettyPrinter().writeValue(new File(args[1], "parsed.json"), out);
    m.writerWithDefaultPrettyPrinter().writeValue(new File(args[1], "utilization.json"), out.utilization);
  }
}
