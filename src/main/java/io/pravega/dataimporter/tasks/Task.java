package io.pravega.dataimporter.tasks;

public interface Task {

    // Order of Execution: Prior Actions, Job, Post Actions
    public void performPriorActions();

    public void runJob();

    public void performPostActions();
}
