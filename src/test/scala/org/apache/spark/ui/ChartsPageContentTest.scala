package org.apache.spark.ui

import it.eng.spark.hpsc.webui._
import org.apache.spark.ui.support.SparkUiTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.xml.Node

/**
 * Tests for [[ChartsPage]] (the "Charts" landing page attached by `sc.attachChartsTab()`).
 *
 * These tests exercise `buildContent` directly (rather than going through Jetty/HTTP), and treat
 * the embedded `<script>` as an opaque string to guard against regressions in the client-side
 * history/CSV-export logic — in particular the bug where fields referenced by
 * `buildHistoryView`/the CSV export were never populated by `recordRowsSnapshot`, silently
 * producing blank columns in the "history" view and in the exported CSV.
 */
class ChartsPageContentTest extends AnyFlatSpec with Matchers with SparkUiTestSupport {

  private def renderChartsPage(sc: org.apache.spark.SparkContext): (Node, String) = {
    sc.attachChartsTab("charts")
    val tab = sc.ui.get.getTabs.find(_.prefix == "charts").getOrElse(fail("charts tab not found"))
    val page = tab.pages.head
    val rendered = page.render(fakeRequest()).head
    val script = (rendered \\ "script").map(_.text).find(_.contains("function poll")).getOrElse(
      fail("embedded charts script not found"))
    (rendered, script)
  }

  /**
   * Extracts top-level `key: ...` identifiers from the first `historyRows.push({ ... })` object
   * literal. Only matches keys immediately preceded by `{` or `,` so that colons belonging to
   * nested ternary expressions (e.g. `totalMemoryMB: cond ? a : b`) are not mistaken for object
   * keys.
   */
  private def historyRowKeys(script: String): Seq[String] = {
    val start = script.indexOf("historyRows.push({")
    start should be >= 0
    val objStart = start + "historyRows.push(".length // keep the leading '{' so the first key matches too
    val objEnd = script.indexOf("});", objStart) + 1
    val objBody = script.substring(objStart, objEnd)
    "[{,]\\s*(\\w+)\\s*:".r.findAllMatchIn(objBody).map(_.group(1)).toSeq
  }

  /** Extracts the quoted header names from the CSV `var headers = [ ... ]` array literal. */
  private def csvHeaderNames(script: String): Seq[String] = {
    val start = script.indexOf("var headers = [")
    start should be >= 0
    val arrStart = start + "var headers = [".length
    val arrBody = script.substring(arrStart, script.indexOf("];", arrStart))
    "'([a-zA-Z0-9_]+)'".r.findAllMatchIn(arrBody).map(_.group(1)).toSeq
  }

  "ChartsPage" should "embed the applicationId in the generated script" in withLocalSparkContext() { sc =>
    val (_, script) = renderChartsPage(sc)
    script should include(s"var APP_ID = '${sc.applicationId}'")
  }

  it should "embed the configured poll interval and rolling window size" in withLocalSparkContext() { sc =>
    val (_, script) = renderChartsPage(sc)
    script should include(s"var pollMs     = ${ChartsPage.PollIntervalSeconds} * 1000")
    script should include(s"var MAX_POINTS = ${ChartsPage.MaxPoints}")
  }

  it should "render the executor metrics table with a header per data column" in withLocalSparkContext() { sc =>
    val (rendered, _) = renderChartsPage(sc)
    val dataHeaders = (rendered \\ "th").filter(_.attribute("data-sort-key").isDefined)
    // The "no rows" placeholder colspan must match the actual number of <th> columns (checkbox +
    // one per data-sort-key column) or the empty-state row will visually misalign.
    val totalColumns = dataHeaders.size + 1
    dataHeaders.size should be > 0
    totalColumns shouldEqual 25
  }

  it should "not have duplicate keys in the historyRows snapshot object (regression)" in withLocalSparkContext() { sc =>
    val (_, script) = renderChartsPage(sc)
    val keys = historyRowKeys(script)
    keys.distinct.size shouldEqual keys.size
  }

  it should "record every field referenced by the CSV export in the historyRows snapshot (regression)" in
    withLocalSparkContext() { sc =>
      val (_, script) = renderChartsPage(sc)
      val keys = historyRowKeys(script).toSet
      val csvOnlyMetadata = Set("timeLabel", "timeIso") // derived at push-time, not part of the row's own metrics
      val csvHeaders = csvHeaderNames(script).filterNot(csvOnlyMetadata.contains)
      val missing = csvHeaders.filterNot(keys.contains)
      missing shouldBe empty
    }

  it should "poll its own same-origin JSON endpoint rather than the public REST API (regression)" in
    withLocalSparkContext() { sc =>
      val (_, script) = renderChartsPage(sc)
      // The dashboard must fetch metrics from its own tab-relative /json endpoint (backed by
      // ChartsPage.renderJson, reading straight from AppStatusStore) instead of making an
      // outbound HTTP request to the public REST API — see renderJson's scaladoc for why.
      script should include("var METRICS_URL")
      script should include("/charts/json")
      script should not include "/api/v1/applications/"
      script should not include "/allexecutors"
    }

  "renderJson" should "serve the same executor metrics shape as the public REST API, read directly from AppStatusStore" in
    withLocalSparkContext() { sc =>
      sc.attachChartsTab("charts")
      val tab = sc.ui.get.getTabs.find(_.prefix == "charts").getOrElse(fail("charts tab not found"))
      val page = tab.pages.head
      val json = page.renderJson(fakeRequest())

      // A local SparkContext with no jobs run yet still reports at least the driver's own
      // "executor" entry — enough to assert the payload is a well-formed JSON array of executor
      // summaries (id/hostPort fields present), matching the shape of
      // `/api/v1/applications/<appId>/executors`.
      import org.json4s.JsonAST.{JArray, JString}
      json shouldBe a[JArray]
      val JArray(elements) = json
      elements should not be empty
      (elements.head \ "id") shouldBe a[JString]
      (elements.head \ "hostPort") shouldBe a[JString]
    }

  /**
   * Regression test for a bug where any `localStorage` failure while saving recorded history
   * caused the in-memory `historyRows` buffer itself to be repeatedly truncated. This is
   * especially harmful behind a reverse proxy such as the YARN ResourceManager/Knox app proxy,
   * where browsers commonly report storage errors (including `QuotaExceededError`, even when the
   * real cause is storage being blocked/partitioned rather than genuinely full) on every write.
   * That made `saveHistory()` shrink the buffer roughly every ~4 polls, so the "Last Nm"/"All
   * history" views showed only a handful of samples, while the "Current session" view (which reads
   * a completely separate, in-memory-only structure, never touched by `saveHistory`) looked
   * perfectly fine. Locally (no proxy) `localStorage` behaves normally and the bug never manifested.
   *
   * The fix decouples the two concerns entirely: `saveHistory()` must never mutate `historyRows`
   * under any circumstance — persistence failures only ever disable further persistence attempts
   * (`localStorageAvailable = false`). The in-memory buffer is bounded solely by HISTORY_MAX_ROWS,
   * applied in `recordRowsSnapshot`, independent of localStorage outcomes.
   */
  "saveHistory" should "never mutate the in-memory history buffer, even when persistence fails (regression)" in
    withLocalSparkContext() { sc =>
      val (_, script) = renderChartsPage(sc)
      val start = script.indexOf("function saveHistory()")
      start should be >= 0
      val body = script.substring(start, script.indexOf("function recordRowsSnapshot", start))

      // No assignment/mutation of historyRows itself may appear inside saveHistory. Reading it
      // (e.g. slicing into a local variable for persistence) is fine — only reassigning or
      // splicing the shared buffer would be a regression.
      body should not include "historyRows ="
      body should not include "historyRows.splice"
      body should not include "historyRows.shift"
      body should not include "historyRows.pop"

      // Any persistence failure must simply give up on further persistence, not touch the buffer.
      body should include("localStorageAvailable = false")
    }

  it should "bound the in-memory history buffer solely by HISTORY_MAX_ROWS, independent of persistence (regression)" in
    withLocalSparkContext() { sc =>
      val (_, script) = renderChartsPage(sc)
      val recordStart = script.indexOf("function recordRowsSnapshot")
      recordStart should be >= 0
      val recordBody = script.substring(recordStart, script.indexOf("function csvValue", recordStart))
      recordBody should include("historyRows.length > HISTORY_MAX_ROWS")
    }

  /**
   * Regression test for browser tabs becoming sluggish/unresponsive on long-running applications:
   * keeping history bounded only by row count (HISTORY_MAX_ROWS) still allows unbounded *time*
   * ranges to accumulate in memory when there are few executors/a long poll interval, eventually
   * degrading the tab. `trimHistoryByAge()` must drop samples older than HISTORY_MAX_AGE_MS (1h)
   * every time new samples are recorded, and `loadHistory()` must apply the same trim to whatever
   * was restored from localStorage on page load (a stale persisted snapshot could be far older
   * than 1h if the tab was closed and reopened much later).
   */
  "trimHistoryByAge" should "drop in-memory history samples older than HISTORY_MAX_AGE_MS (regression)" in
    withLocalSparkContext() { sc =>
      val (_, script) = renderChartsPage(sc)
      script should include("var HISTORY_MAX_AGE_MS = 60 * 60 * 1000")
      script should include("function trimHistoryByAge()")

      // Must run on every recorded snapshot, before the HISTORY_MAX_ROWS safety-net trim.
      val recordStart = script.indexOf("function recordRowsSnapshot")
      recordStart should be >= 0
      val recordBody = script.substring(recordStart, script.indexOf("function csvValue", recordStart))
      recordBody should include("trimHistoryByAge()")
      val ageIdx = recordBody.indexOf("trimHistoryByAge()")
      val rowsIdx = recordBody.indexOf("historyRows.length > HISTORY_MAX_ROWS")
      ageIdx should (be >= 0 and be < rowsIdx)

      // Must also run right after restoring historyRows from localStorage in loadHistory(),
      // so a stale persisted snapshot from a much-earlier session can't reintroduce old data.
      val loadStart = script.indexOf("function loadHistory()")
      loadStart should be >= 0
      val loadEnd = script.indexOf("function saveHistory", loadStart)
      val loadBody = script.substring(loadStart, loadEnd)
      loadBody should include("trimHistoryByAge()")
    }

  /**
   * Regression test for the root cause behind a genuine `QuotaExceededError` seen in practice: on
   * a long-lived, shared browser session (e.g. reused for weeks across many Spark applications
   * through the same YARN ResourceManager/Knox proxy URL), every previously-monitored application
   * leaves behind its own never-reused `hpsc.heap.history.<applicationId>` localStorage entry.
   * Without cleanup these accumulate forever and eventually exhaust the whole origin quota, making
   * even the very first (tiny) write for a brand-new application fail. `loadHistory()` must purge
   * stale entries belonging to other application IDs (but never the current one) before attempting
   * to read/write its own history.
   */
  "loadHistory" should "purge stale history entries left behind by other applications, but keep its own (regression)" in
    withLocalSparkContext() { sc =>
      val (_, script) = renderChartsPage(sc)
      script should include("function purgeStaleHistoryEntries")
      script should include("HISTORY_KEY_PREFIX")
      // Must be invoked from loadHistory, before reading its own key.
      val loadStart = script.indexOf("function loadHistory()")
      loadStart should be >= 0
      val loadEnd = script.indexOf("function saveHistory", loadStart)
      loadEnd should be > loadStart
      val loadBody = script.substring(loadStart, loadEnd)
      loadBody should include("purgeStaleHistoryEntries()")
      // The purge routine itself must never remove the current application's own key.
      val purgeStart = script.indexOf("function purgeStaleHistoryEntries")
      val purgeBody = script.substring(purgeStart, script.indexOf("function loadHistory", purgeStart))
      purgeBody should include("key !== HISTORY_KEY")
    }

  /**
   * Regression test for a bug introduced by switching the metrics source from the public REST API
   * (`/api/v1/applications/<appId>/executors`, active-only) to `AppStatusStore.executorList(false)`
   * (active AND already-terminated executors, so history keeps working across executor deaths —
   * see the "poll its own same-origin JSON endpoint" test above). Once an executor dies, Spark
   * freezes its last-known metrics snapshot in `AppStatusStore` (kept around for History Server
   * use) — every subsequent poll would keep re-reading that same frozen snapshot forever, making a
   * dead executor appear to permanently "hold" its last memory reading on the live charts/table,
   * even though it no longer exists. `poll()` must filter the fetched executor list down to
   * `ex.isActive` truthy entries before updating any live chart series, table row or summary —
   * samples recorded while an executor was alive must remain untouched in `historyRows`, only the
   * *live* view must stop tracking it once dead.
   */
  "poll" should "exclude terminated executors (isActive === false) from the live series/table (regression)" in
    withLocalSparkContext() { sc =>
      val (_, script) = renderChartsPage(sc)
      val pollStart = script.indexOf("function poll()")
      pollStart should be >= 0
      val pollEnd = script.indexOf("/* ── interval control", pollStart)
      pollEnd should be > pollStart
      val pollBody = script.substring(pollStart, pollEnd)

      // The raw fetch result must be filtered by isActive before being assigned to
      // latestExecutorPayload / used to update chart series.
      pollBody should include("payload.data.filter")
      pollBody should include("ex.isActive")
      val filterIdx = pollBody.indexOf("payload.data.filter")
      val assignIdx = pollBody.indexOf("latestExecutorPayload =")
      filterIdx should (be >= 0 and be < assignIdx)
    }

  "ChartsPage layout" should "place Memory on the left and stack GC/CPU/Task Saturation in a single aligned column on the right" in
    withLocalSparkContext() { sc =>
      val (rendered, _) = renderChartsPage(sc)
      val styleText = (rendered \\ "style").map(_.text).headOption.getOrElse(fail("embedded <style> not found"))
      val gridStart = styleText.indexOf(".heap-charts-grid {")
      gridStart should be >= 0
      val gridBody = styleText.substring(gridStart, styleText.indexOf("}", gridStart + ".heap-charts-grid {".length))
      gridBody should include("'memory gc'")
      gridBody should include("'memory cpu'")
      gridBody should include("'memory saturation'")
      // GC must come before CPU which must come before Saturation in the right-hand column order.
      val gcIdx = gridBody.indexOf("'memory gc'")
      val cpuIdx = gridBody.indexOf("'memory cpu'")
      val satIdx = gridBody.indexOf("'memory saturation'")
      gcIdx should (be >= 0 and be < cpuIdx)
      cpuIdx should be < satIdx
    }

  /**
   * Regression/feature test: task saturation (activeTasks/maxTasks) can legitimately exceed 100%
   * (many small tasks oversubscribing cores per executor), so its "unhealthy"/warning threshold
   * must be configurable independently from the heap Warn % field, defaulting to 400% rather than
   * a value close to 100 that would otherwise flag perfectly normal executors as unhealthy.
   */
  "satWarnPct" should "be a separately configurable input defaulting to 400, driving the saturation unhealthy threshold" in
    withLocalSparkContext() { sc =>
      val (rendered, script) = renderChartsPage(sc)
      val satInput = (rendered \\ "input").find(n => (n \ "@id").text == "satWarnPct")
        .getOrElse(fail("satWarnPct input not found"))
      (satInput \ "@value").text shouldEqual "400"
      (satInput \ "@min").text shouldEqual "1"
      (satInput \ "@max").text shouldEqual "2000"

      val isRowUnhealthyStart = script.indexOf("function isRowUnhealthy(row, warnPct, satWarnPct)")
      isRowUnhealthyStart should be >= 0
      val isRowUnhealthyBody =
        script.substring(isRowUnhealthyStart, script.indexOf("function toRow", isRowUnhealthyStart))
      isRowUnhealthyBody should include("row.saturationPct >= satWarnPct")
      isRowUnhealthyBody should not include "row.saturationPct >= 85"

      // toRow/renderTable must forward the configured threshold rather than any hardcoded value.
      script should include("function toRow(ex, warnPct, satWarnPct)")
      script should include("function renderTable(rows, warnPct, satWarnPct)")
    }

  "resolveTotalMemory" should "default to rss mode and never fall back to the estimate, even when rssMB is null" in
    withLocalSparkContext() { sc =>
      val (_, script) = renderChartsPage(sc)

      script should include("var totalMemoryMode = 'rss';")

      val fnStart = script.indexOf("function resolveTotalMemory(heapMB, overheadMB, offHeapMB, rssMB)")
      fnStart should be >= 0
      val fnBody = script.substring(fnStart, script.indexOf("function", fnStart + 1))

      // In rss mode the function must return the raw rssMB (possibly null) and must not
      // substitute the heap+overhead+off-heap estimate as a fallback.
      fnBody should include("if (totalMemoryMode === 'rss') return { value: rssMB, source: rssMB != null ? 'rss' : null };")
      fnBody should not include "rssMB != null ? rssMB :"
    }

  "the total-memory mode toggle" should "always be visible (not hidden until an RSS sample is discovered)" in
    withLocalSparkContext() { sc =>
      val (rendered, script) = renderChartsPage(sc)

      val toggleGroup = (rendered \\ "span").find(n => (n \ "@id").text == "totalMemoryModeGroup")
        .getOrElse(fail("totalMemoryModeGroup not found"))
      (toggleGroup \ "@style").text should not include "display:none"

      val rssRadio = (rendered \\ "input").find(n => (n \ "@id").text == "totalMemoryModeRss")
        .getOrElse(fail("totalMemoryModeRss radio not found"))
      (rssRadio \ "@checked").text shouldEqual "checked"

      // The old auto-reveal function must be gone; visibility is now unconditional.
      script should not include "revealTotalMemoryModeToggleIfNeeded"
    }

  "rssMissingWarning" should "be present in the DOM, hidden by default, and toggled by updateRssAvailabilityWarning" in
    withLocalSparkContext() { sc =>
      val (rendered, script) = renderChartsPage(sc)

      val warningDiv = (rendered \\ "div").find(n => (n \ "@id").text == "rssMissingWarning")
        .getOrElse(fail("rssMissingWarning div not found"))
      (warningDiv \ "@style").text should include("display:none")

      script should include("function updateRssAvailabilityWarning()")
      script should include(
        "warning.style.display = (totalMemoryMode === 'rss' && !processTreeMetricsAvailable) ? 'block' : 'none';")
    }

  "useRss determination" should "be unconditional on totalMemoryMode, not gated by historical RSS presence" in
    withLocalSparkContext() { sc =>
      val (_, script) = renderChartsPage(sc)

      // Both chart-rendering paths must decide useRss purely from the current mode, without
      // requiring that some historical sample already had a non-null RSS value.
      val occurrences = "var useRss = totalMemoryMode === 'rss';".r.findAllMatchIn(script).length
      occurrences shouldEqual 2
      script should not include "series.rssData.some"
    }

  "loadTotalMemoryMode" should "be invoked during bootstrap so the persisted preference is restored on reload" in
    withLocalSparkContext() { sc =>
      val (_, script) = renderChartsPage(sc)
      val bootstrapStart = script.indexOf("/* ── kick off")
      bootstrapStart should be >= 0
      script.substring(bootstrapStart) should include("loadTotalMemoryMode();")
    }

}
