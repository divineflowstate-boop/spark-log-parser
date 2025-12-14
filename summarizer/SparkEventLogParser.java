package com.yourorg.sparklog;

import com.fasterxml.jackson.databind.*;
import java.io.*;
import java.nio.file.*;
import java.util.zip.GZIPInputStream;

/**
 * Spark 3.5.x event log parser (JSON lines).
 * Extracts StageCompleted metrics sufficient for Phase-1/2 (input/shuffle partition size).
 */
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
        JsonNode evNode = n.get("Event");
        if (evNode == null) continue;

        String ev = evNode.asText();

        if (ev.equals("SparkListenerApplicationStart")) {
          if (n.has("App ID")) log.appId = n.get("App ID").asText();
          if (n.has("Timestamp")) log.appStartMs = n.get("Timestamp").asLong();
        }

        if (ev.equals("SparkListenerApplicationEnd")) {
          if (n.has("Timestamp")) log.appEndMs = n.get("Timestamp").asLong();
        }

        if (ev.equals("SparkListenerStageCompleted")) {
          JsonNode s = n.get("Stage Info");
          if (s == null) continue;

          CompactParsedLog.Stage st = new CompactParsedLog.Stage();
          st.stageId = safeInt(s, "Stage ID");
          st.submissionMs = safeLong(s, "Submission Time");
          st.completionMs = safeLong(s, "Completion Time");
          st.executorRunTimeMs = safeLong(s, "Executor Run Time");
          st.gcTimeMs = safeLong(s, "JVM GC Time");

          st.spillBytes = safeLong(s, "Shuffle Spill (Memory)") + safeLong(s, "Shuffle Spill (Disk)");
          st.numTasks = safeInt(s, "Number of Tasks");

          // input bytes
          if (s.has("Input Metrics") && s.get("Input Metrics").has("Bytes Read")) {
            st.inputBytes = s.get("Input Metrics").get("Bytes Read").asLong();
          }

          // shuffle read
          if (s.has("Shuffle Read Metrics") && s.get("Shuffle Read Metrics").has("Total Bytes Read")) {
            st.shuffleReadBytes = s.get("Shuffle Read Metrics").get("Total Bytes Read").asLong();
          }

          // shuffle write
          if (s.has("Shuffle Write Metrics") && s.get("Shuffle Write Metrics").has("Shuffle Bytes Written")) {
            st.shuffleWriteBytes = s.get("Shuffle Write Metrics").get("Shuffle Bytes Written").asLong();
          }

          log.stages.add(st);
        }
      }
    }

    return log;
  }

  private static long safeLong(JsonNode n, String k) {
    return (n.has(k) && !n.get(k).isNull()) ? n.get(k).asLong() : 0L;
  }

  private static int safeInt(JsonNode n, String k) {
    return (n.has(k) && !n.get(k).isNull()) ? n.get(k).asInt() : 0;
  }
}
