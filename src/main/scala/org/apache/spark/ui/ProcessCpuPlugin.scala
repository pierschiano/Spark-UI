package org.apache.spark.ui

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}

import java.lang.management.ManagementFactory
import java.util.concurrent.{Executors, ScheduledExecutorService, ThreadFactory, TimeUnit}
import java.util.{Collections, Map => JMap}
import scala.collection.concurrent.TrieMap

/**
 * Optional [[SparkPlugin]] providing a truly live, OS-level CPU% per executor process, computed
 * straight from the process's own PID via `com.sun.management.OperatingSystemMXBean` — as opposed
 * to [[ExecutorCpuTimeListener]]'s `executorCpuTime`-based reading, which (as documented there)
 * Spark only updates once a task *finishes*, so it cannot reflect CPU consumed by still-running
 * tasks between polls.
 *
 * '''Disabled by default''': Spark plugins are opt-in by design — this class only loads and runs
 * when explicitly listed in `spark.plugins` (e.g.
 * `spark.plugins=org.apache.spark.ui.ProcessCpuPlugin`), so no thread/scheduler/RPC
 * traffic exists unless the user asks for it. `HeapMonitorPage`'s dashboard degrades gracefully
 * (falls back to the existing `executorCpuTime`/`totalDuration`-based estimate) when the plugin
 * isn't loaded.
 *
 * How it works:
 *  - The executor-side component ([[ProcessCpuExecutorPlugin]]) polls
 *    `OperatingSystemMXBean.getProcessCpuTime()` (cumulative process CPU time in ns — monotonic,
 *    unlike the deprecated-for-removal `getProcessCpuLoad()`) on a fixed schedule, computes the
 *    delta-based CPU% since the previous poll (normalized by the number of available cores, same
 *    convention `HeapMonitorPage`'s JS already uses for `cpuTimeMs`/`totalDuration`), and sends
 *    each sample to the driver via [[PluginContext#send]] (fire-and-forget, executor -> driver
 *    only).
 *  - The driver-side component ([[ProcessCpuDriverPlugin]]) receives those samples and keeps only
 *    the latest one per executor ID in [[ProcessCpuPlugin.latestPctByExecutor]]. Since the driver
 *    process itself never runs an `ExecutorPlugin` (that lifecycle is executor-only), it also
 *    polls and records its own process CPU the same way, directly (no RPC needed).
 *  - `ChartsPage.renderJson` reads the latest sample via [[ProcessCpuPlugin.cpuPctFor]] and merges
 *    it into the JSON payload as `osCpuLoadPct`, so the dashboard can prefer it over the
 *    task-completion-granularity estimate whenever it's present.
 *
 * @author Pierluigi Schiano
 */
class ProcessCpuPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new ProcessCpuDriverPlugin
  override def executorPlugin(): ExecutorPlugin = new ProcessCpuExecutorPlugin
}

private[ui] object ProcessCpuPlugin {
  /** Poll interval for the OS-level CPU sampling; deliberately independent of the UI's own poll
   *  interval (`ChartsPage.PollIntervalSeconds`) so a slow/misconfigured UI refresh rate doesn't
   *  affect how fresh this reading is. */
  val PollIntervalMs: Long = 2000L

  // Latest computed CPU% per executor ID (including "driver"), populated by
  // ProcessCpuDriverPlugin. Empty (thus a no-op for readers) unless the plugin is loaded.
  private[ui] val latestPctByExecutor: TrieMap[String, Double] = TrieMap.empty[String, Double]

  /** Latest OS-level process CPU %, or `None` if unavailable (plugin not loaded, or no sample
   *  received yet for this executor). */
  def cpuPctFor(executorId: String): Option[Double] = latestPctByExecutor.get(executorId)

  private[ui] def daemonScheduler(threadName: String): ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor(new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val t = new Thread(r, threadName)
        t.setDaemon(true)
        t
      }
    })

  /** Cumulative process CPU time in ns, or `None` if this JVM doesn't expose the vendor-specific
   *  `com.sun.management.OperatingSystemMXBean` (e.g. a non-HotSpot JVM). */
  private[ui] def processCpuTimeNs(): Option[Long] = ManagementFactory.getOperatingSystemMXBean match {
    case sunBean: com.sun.management.OperatingSystemMXBean =>
      val v = sunBean.getProcessCpuTime
      if (v >= 0) Some(v) else None // -1 means "not available on this platform"
    case _ => None
  }
}

/** Message sent by the executor-side component to report its own process's latest CPU%. */
private[ui] case class ProcessCpuSample(executorId: String, cpuPct: Double)

/**
 * Tracks cumulative CPU time deltas since the previous poll and turns them into a CPU%
 * normalized by the number of available cores. Shared (identical) logic used by both the
 * driver-side self-poll and the executor-side poll below.
 */
private[ui] final class CpuLoadPctTracker {
  private var prevCpuTimeNs: Long = -1L
  private var prevWallNs: Long = -1L

  /** Returns the CPU% consumed since the previous call, or `None` on the first call, when the
   *  underlying OS metric is unavailable, or when the elapsed time is too small to be meaningful. */
  def nextPct(): Option[Double] = {
    val wallNs = System.nanoTime()
    val cpuTimeNsOpt = ProcessCpuPlugin.processCpuTimeNs()
    val result = cpuTimeNsOpt.flatMap { cpuTimeNs =>
      if (prevCpuTimeNs < 0) {
        None
      } else {
        val deltaWallNs = wallNs - prevWallNs
        if (deltaWallNs <= 0) {
          None
        } else {
          val deltaCpuNs = cpuTimeNs - prevCpuTimeNs
          val cores = Runtime.getRuntime.availableProcessors()
          Some((deltaCpuNs.toDouble * 100.0) / (deltaWallNs.toDouble * cores))
        }
      }
    }
    cpuTimeNsOpt.foreach(prevCpuTimeNs = _)
    prevWallNs = wallNs
    result
  }
}

private[ui] class ProcessCpuDriverPlugin extends DriverPlugin {
  private var scheduler: ScheduledExecutorService = _

  override def init(sc: SparkContext, pluginContext: PluginContext): JMap[String, String] = {
    // The driver process never runs an ExecutorPlugin (that lifecycle is executor-only), so poll
    // and record its own CPU load directly here instead - no RPC needed since we're already on
    // the driver.
    val tracker = new CpuLoadPctTracker
    val executorId = pluginContext.executorID()
    scheduler = ProcessCpuPlugin.daemonScheduler("hpsc-process-cpu-plugin-driver")
    scheduler.scheduleWithFixedDelay(
      () => tracker.nextPct().foreach(pct => ProcessCpuPlugin.latestPctByExecutor.put(executorId, pct)),
      ProcessCpuPlugin.PollIntervalMs, ProcessCpuPlugin.PollIntervalMs, TimeUnit.MILLISECONDS)
    Collections.emptyMap()
  }

  override def receive(message: Any): AnyRef = {
    message match {
      case s: ProcessCpuSample => ProcessCpuPlugin.latestPctByExecutor.put(s.executorId, s.cpuPct)
      case _ => // ignore unknown messages
    }
    null
  }

  override def shutdown(): Unit = if (scheduler != null) scheduler.shutdownNow()
}

private[ui] class ProcessCpuExecutorPlugin extends ExecutorPlugin {
  private var scheduler: ScheduledExecutorService = _

  override def init(ctx: PluginContext, extraConf: JMap[String, String]): Unit = {
    val tracker = new CpuLoadPctTracker
    val executorId = ctx.executorID()
    scheduler = ProcessCpuPlugin.daemonScheduler("hpsc-process-cpu-plugin-executor")
    scheduler.scheduleWithFixedDelay(() => {
      tracker.nextPct().foreach { pct =>
        try {
          ctx.send(ProcessCpuSample(executorId, pct))
        } catch {
          case _: Throwable => // best-effort: a dropped sample must never affect task execution
        }
      }
    }, ProcessCpuPlugin.PollIntervalMs, ProcessCpuPlugin.PollIntervalMs, TimeUnit.MILLISECONDS)
  }

  override def shutdown(): Unit = if (scheduler != null) scheduler.shutdownNow()
}
