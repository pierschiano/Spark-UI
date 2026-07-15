package it.eng.spark.hpsc

import org.apache.spark.SparkContext
import org.apache.spark.ui.HpscUI

import javax.servlet.http.HttpServletRequest
import scala.xml.Node

/**
 * Adds the `sc.attachUITab` / `sc.attachUIPage` extensions used to plug custom tabs (and pages)
 * into the Spark UI, exactly like the built-in Jobs/Stages/Storage/Environment tabs.
 *
 * {{{
 *   import it.eng.spark.hpsc.webui._
 *
 *   // new tab, reachable at http://<driver>:4040/hpsc/
 *   sc.attachUITab("hpsc") { request =>
 *     <div>My custom metrics</div>
 *   }
 *
 *   // extra page under the same tab, reachable at http://<driver>:4040/hpsc/details/
 *   sc.attachUIPage("hpsc", "details", "HPSC Details") { request =>
 *     <div>...</div>
 *   }
 * }}}
 *
 * @author Pierluigi Schiano
 */
package object webui {

  implicit class HpscSparkContextFunctions(private val sc: SparkContext) extends AnyVal {

    /**
     * Registers a new tab named `tabName` in the Spark UI whose landing page renders `content`.
     * Calling this again with the same `tabName` (on the same `SparkContext`) does not create a
     * second tab: use [[attachUIPage]] to add further pages to it.
     *
     * @param tabName lower-case, URL-safe tab identifier; also used as the tab's nav bar label
     * @param title   page `<title>` shown in the browser tab; defaults to the capitalized `tabName`
     * @param content the HTML body of the page, given the incoming request
     */
    def attachUITab(tabName: String, title: String = null)
                   (content: HttpServletRequest => Seq[Node]): Unit =
      HpscUI.attachPage(sc, tabName, "", Option(title).getOrElse(tabName.capitalize))(content)

    /**
     * Attaches an additional page to a `tabName` tab (creating it first if needed), reachable at
     * `.../<tabName>/<pagePrefix>/`.
     *
     * @param tabName    the tab to attach the page to
     * @param pagePrefix URL-safe page identifier within the tab
     * @param title      page `<title>` shown in the browser tab; defaults to the capitalized `pagePrefix`
     * @param content    the HTML body of the page, given the incoming request
     */
    def attachUIPage(tabName: String, pagePrefix: String, title: String = null)
                    (content: HttpServletRequest => Seq[Node]): Unit =
      HpscUI.attachPage(sc, tabName, pagePrefix, Option(title).getOrElse(pagePrefix.capitalize))(content)

    /**
     * Attaches a "Charts" tab to the Spark UI.
     *
     * The tab shows a live time-series chart of JVM heap usage (MB) for every active executor,
     * updated every few seconds by polling the Spark REST API. It also exposes operational
     * metrics such as GC time, active/max tasks, task saturation, failed tasks, shuffle
     * read/write, storage usage, top offenders, unhealthy-only filtering, sortable executor
     * tables and secondary charts for GC and task saturation. Reachable at
     * {{{
     *   http://<driver>:4040/<tabName>/
     * }}}
     *
     * Calling this method more than once with the same `tabName` on the same `SparkContext` is a
     * no-op.
     *
     * {{{
     *   import it.eng.spark.hpsc.webui._
     *
     *   sc.attachChartsTab()                       // tab label: "charts"
     *   sc.attachChartsTab(tabName = "my-metrics") // custom tab name
     * }}}
     *
     * @param tabName URL-safe tab identifier; also used as the nav-bar label (default `"charts"`)
     */
    def attachChartsTab(tabName: String = "charts"): Unit =
      HpscUI.attachCharts(sc, tabName)

  }

}
