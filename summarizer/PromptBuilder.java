package com.yourorg.sparklog;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Builds an LLM prompt that is grounded in deterministic diff facts.
 * You can replace this with your org's GPT-4o calling layer later.
 */
public class PromptBuilder {

  public static String buildPrompt(RunDiff diff) throws Exception {
    ObjectMapper om = new ObjectMapper();
    String facts = om.writerWithDefaultPrettyPrinter().writeValueAsString(diff);

    return """
You are an internal Spark + Reconciliation observability assistant.

Rules:
- Use ONLY the provided facts.
- Do NOT invent configs or cluster sizes.
- If a fact is missing, say it's missing.
- Provide actionable recommendations.

Tasks:
1) Summarize whether performance regressed/improved and why (bytes/tasks/partitions/GC/spill).
2) Summarize match-quality change (overall + key rules).
3) Call out precision vs recall pattern.
4) Provide deterministic tuning suggestions:
   - input partitions (coalesce/repartition multiplier)
   - shuffle partitions (spark.sql.shuffle.partitions multiplier)
   - executor sizing hints (if under-utilized)

Facts JSON:
""" + facts;
  }
}
