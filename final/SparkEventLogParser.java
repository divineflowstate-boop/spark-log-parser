
package com.yourorg.sparklog;

import com.fasterxml.jackson.databind.*;
import java.io.*;
import java.nio.file.*;
import java.util.zip.GZIPInputStream;

public class SparkEventLogParser {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public CompactParsedLog parse(Path path) throws Exception {
    InputStream in = Files.newInputStream(path);
    if (path.toString().endsWith(".gz")) {
      in = new GZIPInputStream(in);
    }

    CompactParsedLog log = new CompactParsedLog();

    try (BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
      String line;
      while ((line = br.readLine()) != null) {
        JsonNode n = MAPPER.readTree(line);
        String type = n.get("Event").asText();

        if ("SparkListenerApplicationStart".equals(type)) {
          log.appId = n.get("App ID").asText();
          log.startTimeMs = n.get("Timestamp").asLong();
        }

        if ("SparkListenerApplicationEnd".equals(type)) {
          log.endTimeMs = n.get("Timestamp").asLong();
        }

        if ("SparkListenerStageCompleted".equals(type)) {
          JsonNode s = n.get("Stage Info");
          CompactParsedLog.StageInfo si = new CompactParsedLog.StageInfo();
          si.stageId = s.get("Stage ID").asInt();
          si.submissionTimeMs = s.get("Submission Time").asLong();
          si.completionTimeMs = s.get("Completion Time").asLong();
          si.executorRunTime = s.get("Executor Run Time").asLong();
          si.gcTime = s.get("JVM GC Time").asLong();
          si.spillBytes = s.get("Shuffle Spill (Memory)").asLong()
                        + s.get("Shuffle Spill (Disk)").asLong();
          si.numTasks = s.get("Number of Tasks").asInt();
          log.stages.put(si.stageId, si);
        }
      }
    }
    return log;
  }
}
