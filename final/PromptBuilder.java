
package com.yourorg.sparklog;

public class PromptBuilder {

  public static String buildPrompt(RunDiff diff) {
    return "You are an internal Spark observability assistant.\n" +
           "Explain what changed, why, and what to do next.\n\n" +
           diff.toString();
  }
}
