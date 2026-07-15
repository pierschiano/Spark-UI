package org.apache.spark.ui

import org.apache.spark.SparkContext
import org.apache.spark.ui.{SparkUI, SparkUITab, UIUtils, WebUIPage}

import javax.servlet.http.HttpServletRequest
import scala.collection.mutable
import scala.xml.Node

/**
 * Custom tab attached to the driver's Spark UI, next to the built-in Jobs/Stages/Storage/
 * Environment/Executors tabs.
 *
 * `SparkUI`/`SparkUITab`/`WebUIPage` and `SparkContext#ui` are all `private[spark]`. This class
 * (and [[HpscUIPage]]/[[HpscUI]]) therefore live under the `org.apache.spark` package tree to
 * access them, following the same pattern used by `org.apache.spark.exa.hpsc.DistributePartitionRDD`
 * in the `common` module. The class itself is otherwise a plain (public) type.
 *
 * @author Pierluigi Schiano
 */
class HpscUITab private[ui](parent: SparkUI, tabName: String) extends SparkUITab(parent, tabName)

/**
 * A single page of an [[HpscUITab]]. The HTML body is produced by the caller-supplied `content`
 * function; this class only wraps it with the standard Spark UI chrome (header, tab bar, css/js)
 * via `UIUtils.headerSparkPage`.
 */
class HpscUIPage private[ui](tab: HpscUITab, prefix: String, title: String)
                            (content: HttpServletRequest => Seq[Node]) extends WebUIPage(prefix) {

  override def render(request: HttpServletRequest): Seq[Node] =
    UIUtils.headerSparkPage(request, title, content(request), tab.asInstanceOf[SparkUITab])

}

/**
 * Entry point used by the public `it.eng.spark.hpsc.webui` API to attach custom tabs/pages
 * to a `SparkContext`'s web UI, without leaking any `private[spark]` type outside this package.
 */
object HpscUI {

  // One tab instance per (SparkContext, tabName). SparkContext keys are weak to avoid
  // retaining disposed contexts in long-lived driver processes.
  private val tabsByContext = mutable.WeakHashMap.empty[SparkContext, mutable.Map[String, HpscUITab]]

  private def tabMap(sc: SparkContext): mutable.Map[String, HpscUITab] =
    tabsByContext.getOrElseUpdate(sc, mutable.Map.empty[String, HpscUITab])

  /**
   * Attaches a ready-made "Charts" tab to the Spark UI.
   *
   * The tab is registered under `tabName` (default `"charts"`) and exposes a single landing
   * page with a client-side time-series heap chart that polls
   * {{{
   *   /api/v1/applications/<appId>/executors
   * }}}
   * every few seconds and plots `peakMemoryMetrics.JVMHeapMemory` (or
   * `memoryMetrics.usedOnHeapStorageMemory` as fallback) for each active executor, plus
   * operational metrics such as task saturation, failed tasks, GC time, shuffle read/write,
   * storage usage, top-offender summaries, unhealthy-only filtering and client-side sorting.
   *
   * Calling this method a second time with the same `SparkContext` and `tabName` is a no-op
   * (the tab is already registered).
   *
   * @param sc      the SparkContext whose UI the tab is attached to
   * @param tabName URL-safe tab identifier; also used as the nav-bar label
   */
  def attachCharts(sc: SparkContext, tabName: String = "charts"): Unit = synchronized {
    val ui = sc.ui.getOrElse(
      throw new IllegalStateException("Spark UI is disabled (set spark.ui.enabled=true)"))

    tabMap(sc).getOrElseUpdate(tabName, {
      val newTab = new HpscUITab(ui, tabName)
      ui.attachTab(newTab)
      // Accumulates real per-executor CPU time from completed tasks (see
      // ExecutorCpuTimeListener) so ChartsPage.renderJson can expose it as `cpuTimeMs`, giving the
      // dashboard's CPU % a real measurement instead of only a wall-clock-based estimate.
      val cpuTimeListener = new ExecutorCpuTimeListener
      sc.addSparkListener(cpuTimeListener)
      val page = new ChartsPage(newTab, sc, cpuTimeListener)
      newTab.attachPage(page)
      ui.attachPage(page)
      newTab
    })
    ()
  }

  /**
   * Attaches a page at `.../<tabName>/<pagePrefix>/` to the Spark UI, creating and registering
   * the `tabName` tab itself the first time it is used.
   *
   * @param sc         the SparkContext whose UI the page is attached to
   * @param tabName    URL-safe tab identifier; also used as the tab's label in the nav bar
   * @param pagePrefix URL-safe page identifier within the tab; use `""` for the tab's landing page
   * @param title      page `<title>` / browser tab title
   * @param content    the HTML body of the page, given the incoming request
   */
  def attachPage(sc: SparkContext, tabName: String, pagePrefix: String, title: String)
                (content: HttpServletRequest => Seq[Node]): Unit = synchronized {
    val ui = sc.ui.getOrElse(
      throw new IllegalStateException("Spark UI is disabled (set spark.ui.enabled=true)"))

    val tab = tabMap(sc).getOrElseUpdate(tabName, {
      val newTab = new HpscUITab(ui, tabName)
      ui.attachTab(newTab) // registers the tab in the header nav bar (no pages attached yet)
      newTab
    })

    val page = new HpscUIPage(tab, pagePrefix, title)(content)
    tab.attachPage(page) // WebUITab bookkeeping: prefixes the page path and tracks it on the tab
    ui.attachPage(page) // WebUI: actually registers the servlet handler for the page's path
  }

}
