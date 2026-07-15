package org.apache.spark.ui

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}

import scala.collection.concurrent.TrieMap

/**
 * Tracks real, cumulative per-executor CPU time consumed by completed tasks, in nanoseconds.
 *
 * `TaskMetrics.executorCpuTime` (populated by every task via `ManagementFactory.getThreadMXBean`
 * thread-CPU-time deltas around the task body) is the same real measurement Spark's own built-in
 * `ExecutorSource` "cpuTime" dropwizard counter is derived from — but that counter is only
 * reachable through the separate `/metrics/json` sink, not through the executors REST API /
 * `AppStatusStore.executorList` that [[ChartsPage]] polls (neither `ExecutorSummary` nor
 * `ExecutorStageSummary` carry a CPU-time field). This listener re-derives the same real value by
 * accumulating `onTaskEnd` events directly, so it can be exposed alongside the regular executor
 * metrics payload (see [[ChartsPage.renderJson]]) without requiring a second data source / poll.
 *
 * One instance is registered per `SparkContext` (see `HpscUI.attachCharts`) via
 * `sc.addSparkListener`. `onTaskEnd` is dispatched from the single-threaded `AsyncEventQueue`
 * thread, so the accumulation itself never races with itself; the `synchronized` block below only
 * serializes that one writer against concurrent readers (Jetty request threads calling
 * [[cpuTimeMsFor]]) — `TrieMap` reads are lock-free regardless.
 *
 * @author Pierluigi Schiano
 */
private[ui] class ExecutorCpuTimeListener extends SparkListener {

  private val cpuTimeNsByExecutor = TrieMap.empty[String, Long]

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val executorId = if (taskEnd.taskInfo != null) taskEnd.taskInfo.executorId else null
    val cpuNs = Option(taskEnd.taskMetrics).map(_.executorCpuTime).getOrElse(0L)
    if (executorId != null && cpuNs > 0) {
      cpuTimeNsByExecutor.synchronized {
        cpuTimeNsByExecutor(executorId) = cpuTimeNsByExecutor.getOrElse(executorId, 0L) + cpuNs
      }
    }
  }

  /** Cumulative real CPU time consumed so far by `executorId`'s completed tasks, in milliseconds. */
  def cpuTimeMsFor(executorId: String): Long = cpuTimeNsByExecutor.getOrElse(executorId, 0L) / 1000000L

}
