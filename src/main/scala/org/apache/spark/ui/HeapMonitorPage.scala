package org.apache.spark.ui

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonUnwrapped
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.SparkContext
import org.apache.spark.status.api.v1.{ExecutorSummary, JacksonMessageWriter}
import org.apache.spark.ui.{SparkUITab, UIUtils, WebUIPage}
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods

import javax.servlet.http.HttpServletRequest
import scala.xml.{Node, Unparsed}

/**
 * A [[WebUIPage]] that shows a live time-series chart of JVM heap usage (MB) for every
 * executor. The page is rendered entirely client-side without external assets: a
 * JavaScript `setInterval` loop polls this page's own `.../json` endpoint (see [[renderJson]])
 * every [[ChartsPage.PollIntervalSeconds]] seconds, reads
 * `peakMemoryMetrics.JVMHeapMemory` (falling back to
 * `memoryMetrics.usedOnHeapStorageMemory` when the peak field is absent), and appends the
 * sample to a rolling window of [[ChartsPage.MaxPoints]] points per executor.
 *
 * Besides the heap chart, the page exposes additional operational metrics taken from the same
 * executor metrics payload: active/max tasks, task saturation, failed tasks, GC time / GC ratio,
 * shuffle read / write, storage counters, unhealthy-only filtering, client-side sorting and
 * top-offender summaries.
 *
 * The page is registered under the tab's root prefix (`""`), so it becomes the tab's landing page.
 *
 * @param tab the [[HpscUITab]] this page belongs to
 * @param sc  the running [[SparkContext]] (used to read `applicationId` and the executor metrics)
 * @param cpuTimeListener accumulates real per-executor CPU time from completed tasks (see
 *                        [[ExecutorCpuTimeListener]]); merged into each executor's JSON payload as
 *                        `cpuTimeMs` so the client can compute a real (not wall-clock-estimated)
 *                        CPU utilization %.
 * @author Pierluigi Schiano
 */
class ChartsPage private[ui](tab: HpscUITab, sc: SparkContext, cpuTimeListener: ExecutorCpuTimeListener)
  extends WebUIPage("") {

  override def render(request: HttpServletRequest): Seq[Node] =
    UIUtils.headerSparkPage(request, "Charts", buildContent(request), tab.asInstanceOf[SparkUITab])

  // Configured the same way as the (package-private) mapper Spark's own REST API
  // (org.apache.spark.status.api.v1.JacksonMessageWriter) uses to serialize ExecutorSummary —
  // same Jackson + jackson-module-scala setup and ISO date format — so the JSON shape served by
  // renderJson below matches exactly what `/api/v1/applications/<appId>/executors` would have
  // produced (plus the additional `cpuTimeMs` field, flattened via @JsonUnwrapped below), without
  // going through an actual HTTP round-trip / JAX-RS resource dispatch.
  private val jsonMapper: ObjectMapper = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
    mapper.setDateFormat(JacksonMessageWriter.makeISODateFormat)
    mapper
  }

  /**
   * Serves the executor metrics polled by the client-side dashboard, automatically wired up by
   * Spark's `WebUI.attachPage` at `.../<tabPrefix>/json` (every [[WebUIPage]] gets this for free).
   *
   * Reads directly from the driver's own `AppStatusStore` — the exact same in-process data
   * structure the built-in `/api/v1/applications/<appId>/executors` REST endpoint itself
   * serializes — instead of the dashboard making an outbound HTTP request to that REST endpoint.
   * This avoids an extra HTTP round-trip through whatever reverse proxy sits in front of the
   * Spark UI (YARN ResourceManager proxy, Knox, ...) and returns both active AND already-finished
   * executors in a single call (`activeOnly = false`), matching what `/allexecutors` used to add
   * as a fallback.
   *
   * Each element also carries a real `cpuTimeMs` field (cumulative CPU time consumed by that
   * executor's completed tasks so far, in ms — see [[ExecutorCpuTimeListener]]), flattened onto
   * the plain `ExecutorSummary` fields via `@JsonUnwrapped` so the payload stays a JSON array of
   * (almost) plain `ExecutorSummary` objects, just like the REST endpoint, plus this one addition.
   */
  override def renderJson(request: HttpServletRequest): JValue = {
    val executorSummaries = sc.statusStore.executorList(/* activeOnly = */ false)
    val withCpuTime = executorSummaries.map { es =>
      ExecutorSummaryWithCpuTime(es, cpuTimeListener.cpuTimeMsFor(es.id), ProcessCpuPlugin.cpuPctFor(es.id))
    }
    JsonMethods.parse(jsonMapper.writeValueAsString(withCpuTime))
  }


  // ---------------------------------------------------------------------------
  // HTML / JS
  // ---------------------------------------------------------------------------

  private[ui] def buildContent(request: HttpServletRequest): Seq[Node] = {
    val appId = sc.applicationId
    val sparkMemoryFraction = sc.getConf.getDouble("spark.memory.fraction", 0.6d)
    val reservedSystemMemoryBytes = 300L * 1024L * 1024L
    // Same-origin endpoint attached by WebUI.attachPage (see renderJson above); prepending the
    // Spark base URI keeps it working behind /proxy/... and history-server paths, exactly like
    // the page itself.
    val metricsUrl = UIUtils.prependBaseUri(request, s"/${tab.prefix}/json")

    <div style="padding: 10px;">
      <style>
        .heap-summary-grid {{
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
          gap: 12px;
          margin: 16px 0;
        }}
        .heap-summary-card {{
          border: 1px solid #ddd;
          border-radius: 6px;
          padding: 10px 12px;
          background: #fafafa;
        }}
        .heap-summary-title {{
          color: #666;
          font-size: 12px;
          text-transform: uppercase;
          letter-spacing: 0.03em;
          margin-bottom: 4px;
        }}
        .heap-summary-value {{
          font-size: 22px;
          font-weight: bold;
          line-height: 1.2;
        }}
        .heap-summary-note {{
          color: #888;
          font-size: 12px;
          margin-top: 2px;
        }}
        .heap-legend {{
          display: flex;
          flex-wrap: wrap;
          gap: 8px;
          margin-top: 8px;
        }}
        .heap-legend-item {{
          display: inline-flex;
          align-items: center;
          gap: 6px;
          border: 1px solid #ddd;
          border-radius: 12px;
          padding: 4px 8px;
          background: #fff;
          font-size: 12px;
        }}
        .heap-legend-color {{
          width: 10px;
          height: 10px;
          border-radius: 50%;
          display: inline-block;
        }}
        .heap-charts-grid {{
          display: grid;
          grid-template-columns: 2fr 1fr;
          grid-template-areas:
            'memory gc'
            'memory cpu'
            'memory saturation';
          gap: 16px;
          align-items: start;
        }}
        .heap-charts-grid.layout-vertical {{
          grid-template-columns: 1fr;
          grid-template-areas:
            'memory'
            'gc'
            'cpu'
            'saturation';
        }}
        .heap-charts-grid.layout-focus-memory {{
          grid-template-columns: 2fr 1fr;
          grid-template-areas:
            'memory memory'
            'cpu gc'
            'cpu saturation';
        }}
        .heap-charts-grid.layout-focus-cpu {{
          grid-template-columns: 2fr 1fr;
          grid-template-areas:
            'cpu cpu'
            'memory gc'
            'memory saturation';
        }}
        .heap-charts-grid.layout-focus-gc {{
          grid-template-columns: 2fr 1fr;
          grid-template-areas:
            'memory gc'
            'cpu gc'
            'saturation gc';
        }}
        .heap-charts-grid.layout-focus-saturation {{
          grid-template-columns: 2fr 1fr;
          grid-template-areas:
            'memory saturation'
            'cpu saturation'
            'gc saturation';
        }}
        #heapChartContainer {{ grid-area: memory; }}
        #cpuChartContainer {{ grid-area: cpu; }}
        #gcChartContainer {{ grid-area: gc; }}
        #saturationChartContainer {{ grid-area: saturation; }}
        .heap-chart-panel {{
          border: 1px solid #ddd;
          border-radius: 6px;
          background: #fff;
          padding: 8px;
        }}
        .heap-chart-title {{
          font-weight: bold;
          margin: 0 0 6px 0;
        }}
        .heap-topoffenders-grid {{
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
          gap: 12px;
          margin: 16px 0 10px 0;
        }}
        .heap-topoffender-card {{
          border: 1px solid #ddd;
          border-radius: 6px;
          background: #fff;
          padding: 10px 12px;
        }}
        .heap-topoffender-title {{
          font-weight: bold;
          margin-bottom: 8px;
        }}
        .heap-topoffender-list {{
          margin: 0;
          padding-left: 18px;
        }}
        .heap-topoffender-list li {{
          margin: 4px 0;
        }}
        .heap-table-sortable {{
          cursor: pointer;
          white-space: nowrap;
          user-select: none;
        }}
        .heap-table-sortable:hover {{
          background: #f5f5f5;
        }}
        .heap-table-row-unhealthy {{
          background: #fcf8e3;
        }}
        .heap-cell-changed-up {{
          animation: heapCellGlowUp 1.2s ease-out;
        }}
        .heap-cell-changed-down {{
          animation: heapCellGlowDown 1.2s ease-out;
        }}
        @keyframes heapCellGlowUp {{
          0% {{ box-shadow: inset 0 0 0 9999px rgba(46, 204, 113, 0.28); }}
          100% {{ box-shadow: inset 0 0 0 9999px rgba(46, 204, 113, 0.0); }}
        }}
        @keyframes heapCellGlowDown {{
          0% {{ box-shadow: inset 0 0 0 9999px rgba(241, 196, 15, 0.30); }}
          100% {{ box-shadow: inset 0 0 0 9999px rgba(241, 196, 15, 0.0); }}
        }}
        .heap-empty-state {{
          color: #888;
          font-style: italic;
        }}
        @media (max-width: 1200px) {{
          .heap-charts-grid {{
            grid-template-columns: 1fr;
            grid-template-areas:
              'memory'
              'gc'
              'cpu'
              'saturation';
          }}
        }}
        .chart-tooltip {{
          position: fixed;
          display: none;
          pointer-events: none;
          z-index: 1000;
          background: rgba(30, 30, 30, 0.92);
          color: #fff;
          border-radius: 5px;
          padding: 6px 10px;
          font-size: 12px;
          line-height: 1.5;
          max-width: 280px;
          box-shadow: 0 2px 8px rgba(0,0,0,0.35);
        }}
        .chart-tooltip-row {{
          display: flex;
          align-items: center;
          white-space: nowrap;
        }}
        .chart-tooltip-dot {{
          display: inline-block;
          width: 8px;
          height: 8px;
          border-radius: 50%;
          margin-right: 6px;
          flex: 0 0 auto;
        }}
      </style>

      <div id="chartTooltip" class="chart-tooltip"></div>

      <div style="display:flex; gap:16px; margin-bottom:12px; align-items:center;">
        <label for="pollInterval" style="font-weight:bold;">Refresh interval (s):</label>
        <input id="pollInterval" type="number" min="1" max="60" value={ChartsPage.PollIntervalSeconds.toString}
               style="width:70px;" onchange="updateInterval()"/>
        <label for="onlyUnhealthy" style="margin-left:8px;">
          <input id="onlyUnhealthy" type="checkbox" onchange="applyOptions()"/>
          Only unhealthy
        </label>
        <label for="warnPct" style="margin-left:8px;">Warn %:</label>
        <input id="warnPct" type="number" min="1" max="100" value="85" style="width:70px;" onchange="applyOptions()"/>
        <label for="satWarnPct" style="margin-left:8px;" title="Task saturation is activeTasks/maxTasks and can legitimately exceed 100% (many small tasks oversubscribing cores), so its warning threshold is configured separately from heap Warn %.">Sat Warn %:</label>
        <input id="satWarnPct" type="number" min="1" max="2000" value="400" style="width:70px;" onchange="applyOptions()"/>
        <label for="recordHistory" style="margin-left:8px;">
          <input id="recordHistory" type="checkbox" checked="checked"/>
          Record history
        </label>
        <button id="downloadHistory" type="button" class="btn btn-xs btn-default">Download CSV</button>
        <button id="resetHistory" type="button" class="btn btn-xs btn-default">Reset history</button>
        <span id="historyStatus" style="color:#888; font-size:12px;">history: 0 rows</span>
        <span id="lastUpdate" style="color:#888;">–</span>
      </div>
      <div style="display:flex; gap:12px; margin-bottom:10px; align-items:center; flex-wrap:wrap;">
        <span style="font-weight:bold; font-size:12px;">Exec monitor:</span>
        <label style="margin:0;">
          <input type="radio" name="monitorMode" id="monitorModeAll" value="all" checked="checked" onchange="applyOptions()"/>
          All
        </label>
        <label style="margin:0;">
          <input type="radio" name="monitorMode" id="monitorModeSelected" value="selected" onchange="applyOptions()"/>
          Selected only
        </label>
        <button id="selectAllVisibleExec" type="button" class="btn btn-xs btn-default">Select visible</button>
        <button id="clearSelectedExec" type="button" class="btn btn-xs btn-default">Clear selection</button>
        <span id="selectedExecInfo" style="color:#666; font-size:12px;">selected: 0</span>
      </div>
      <div style="display:flex; gap:6px; margin-bottom:10px; align-items:center; flex-wrap:wrap;">
        <span style="font-weight:bold; font-size:12px; margin-right:4px;">View:</span>
        <button data-vr="session" class="btn btn-xs btn-primary heap-vr-btn">Current session</button>
        <button data-vr="5"       class="btn btn-xs btn-default heap-vr-btn">Last 5m</button>
        <button data-vr="15"      class="btn btn-xs btn-default heap-vr-btn">Last 15m</button>
        <button data-vr="30"      class="btn btn-xs btn-default heap-vr-btn">Last 30m</button>
        <button data-vr="60"      class="btn btn-xs btn-default heap-vr-btn">Last 1h</button>
        <button data-vr="all"     class="btn btn-xs btn-default heap-vr-btn" title="In-memory history is retained for the last 1h only, to keep the browser tab responsive on long-running applications.">All history (max 1h)</button>
        <span id="histViewInfo" style="color:#666; font-size:12px; margin-left:8px;"></span>
      </div>
      <div style="display:flex; gap:10px; margin-bottom:10px; align-items:center; flex-wrap:wrap; font-size:12px;">
        <strong>Layout:</strong>
        <select id="layoutPreset" onchange="applyLayoutControls()" style="height:24px;">
          <option value="default">Default</option>
          <option value="vertical">Vertical</option>
          <option value="focus-memory">Focus Memory</option>
          <option value="focus-cpu">Focus CPU</option>
          <option value="focus-gc">Focus GC</option>
          <option value="focus-saturation">Focus Saturation</option>
        </select>
        <label style="margin:0;">Mem H <input id="memoryChartHeight" type="number" min="180" max="900" value="650" style="width:70px;" onchange="applyLayoutControls()"/></label>
        <label style="margin:0;">CPU H <input id="cpuChartHeight" type="number" min="180" max="900" value="210" style="width:70px;" onchange="applyLayoutControls()"/></label>
        <label style="margin:0;">GC H <input id="gcChartHeight" type="number" min="140" max="700" value="210" style="width:70px;" onchange="applyLayoutControls()"/></label>
        <label style="margin:0;">Sat H <input id="satChartHeight" type="number" min="140" max="700" value="210" style="width:70px;" onchange="applyLayoutControls()"/></label>
        <button id="resetLayoutBtn" type="button" class="btn btn-xs btn-default">Reset layout</button>
      </div>

      <div id="heapStatus" style="margin-bottom:10px; color:#666;"></div>
      <div id="heapApiStatus" style="margin-bottom:10px; color:#888; font-size:12px;"></div>
      <div id="heapPeakWarning" style="display:none; margin-bottom:8px; padding:6px 12px; background:#f0f4ff; border:1px solid #c8d4f0; border-radius:4px; color:#555; font-size:12px;">
        ℹ Heap metric: <code>peakMemoryMetrics.JVMHeapMemory</code>
        . This value represents the maximum heap peak observed for each executor.
        In CDP, <code>executorMetrics.JVMHeapMemory</code> is typically not populated by the driver.
      </div>
      <div id="heapSummary" class="heap-summary-grid"></div>
      <div id="heapTopOffenders" class="heap-topoffenders-grid"></div>

      <div class="heap-charts-grid layout-default" id="heapChartsGrid">
        <div class="heap-chart-panel" id="heapChartContainer" style="width:100%; overflow-x:auto;">
          <div class="heap-chart-title">Memory</div>
          <div style="display:flex; gap:12px; align-items:center; flex-wrap:wrap; margin-bottom:8px; font-size:12px; color:#555;">
            <strong>Show:</strong>
            <label style="margin:0;"><input id="heapMetricHeap" type="checkbox" checked="checked" onchange="applyOptions()"/> Heap</label>
            <label style="margin:0;"><input id="heapMetricOverhead" type="checkbox" checked="checked" onchange="applyOptions()"/> Overhead</label>
            <label style="margin:0;"><input id="heapMetricOffHeap" type="checkbox" checked="checked" onchange="applyOptions()"/> Spark Off-Heap</label>
            <label style="margin:0;" title="Gap tra RSS reale e la somma Heap+Overhead+Spark Off-Heap: stack thread, buffer nativi/diretti, overhead allocator nativi. Visibile solo in modalità RSS.">
              <input id="heapMetricNativeOther" type="checkbox" checked="checked" onchange="applyOptions()"/> Native/Other
            </label>
            <label style="margin:0;"><input id="heapMetricTotal" type="checkbox" checked="checked" onchange="applyOptions()"/> Total</label>
            <span id="totalMemoryModeGroup" style="margin-left:8px; border-left:1px solid #ddd; padding-left:12px;">
              <strong>Total source:</strong>
              <label style="margin:0 6px 0 4px;">
                <input type="radio" name="totalMemoryMode" id="totalMemoryModeRss" value="rss" checked="checked" onchange="setTotalMemoryMode(this.value)"/> RSS reale (OS)
              </label>
              <label style="margin:0;">
                <input type="radio" name="totalMemoryMode" id="totalMemoryModeEstimated" value="estimated" onchange="setTotalMemoryMode(this.value)"/> Stima (heap+overhead+off-heap)
              </label>
            </span>
          </div>
          <div id="totalMemoryModeNote" style="font-size:11px; color:#888; margin:-4px 0 8px;">
            ℹ Default: RSS reale rilevata da <code>peakMemoryMetrics.ProcessTreeJVMRSSMemory</code>
            (richiede <code>spark.executor.processTreeMetrics.enabled=true</code>) — memoria fisica reale del
            processo JVM misurata dall'OS, più affidabile della stima per capire i limiti YARN. In modalità RSS
            <strong>non c'è fallback</strong> alla stima: se il campione RSS manca, il "Total" per quel punto
            resta vuoto (gap nel grafico) invece di sostituirlo silenziosamente con la stima.
            In modalità RSS il "Total" quasi sempre <strong>non coincide</strong> con la somma di Heap+Overhead+
            Spark Off-Heap mostrata nello stack: la differenza è resa esplicita come segmento
            <strong>"Native/Other" (N)</strong>. Nota: Metaspace/Code Cache/Compressed Class Space sono già
            inclusi in "Overhead" (JVMOffHeapMemory); "N" copre ciò che NESSUN contatore JVM/Spark misura: stack
            dei thread, buffer diretti/nativi (Netty, Snappy/Zstd, Arrow, JNI), overhead degli allocator nativi
            (glibc malloc arenas) e strutture interne della JVM non esposte da <code>MemoryMXBean</code> —
            proprio la memoria che YARN considera per i limiti del container ma che i contatori H/O/F non vedono.
          </div>
          <div id="rssMissingWarning" style="display:none; margin:-4px 0 8px; padding:6px 12px; background:#fdf4e3; border:1px solid #f0d9a8; border-radius:4px; color:#8a6d3b; font-size:12px;">
            ⚠ Nessun campione RSS reale ricevuto finora. Il "Total" in modalità RSS resterà vuoto finché
            <code>spark.executor.processTreeMetrics.enabled=true</code> non è impostato su tutti gli executor
            (richiede il riavvio dell'applicazione). Passa a "Stima" qui sopra solo per debug temporaneo.
          </div>
          <canvas id="heapChart"></canvas>
          <div id="heapLegend" class="heap-legend"></div>
        </div>
        <div class="heap-chart-panel" id="cpuChartContainer" style="width:100%; overflow-x:auto;">
          <div class="heap-chart-title">CPU % (utilization)</div>
          <canvas id="cpuChart"></canvas>
        </div>
        <div class="heap-chart-panel" id="gcChartContainer" style="width:100%; overflow-x:auto;">
          <div class="heap-chart-title">GC ∆ (ms)</div>
          <canvas id="gcChart"></canvas>
        </div>
        <div class="heap-chart-panel" id="saturationChartContainer" style="width:100%; overflow-x:auto;">
          <div class="heap-chart-title">Task Saturation %</div>
          <canvas id="saturationChart"></canvas>
        </div>
      </div>

      <h4 style="margin-top:24px;">Current executor metrics</h4>
      <table class="table table-bordered table-condensed" id="heapTable" style="width:auto;">
        <thead>
          <tr>
            <th title="Executor selection used by charts when 'Selected only' is active"><input id="monitorAllCheckbox" type="checkbox"/></th>
            <th class="heap-table-sortable" data-sort-key="id" data-label="Executor">Executor</th>
            <th class="heap-table-sortable" data-sort-key="host" data-label="Host">Host</th>
            <th class="heap-table-sortable" data-sort-key="totalMemoryMB" data-label="Total Memory (MB) / JVM Heap Max est. (MB)" id="totalMemoryHeader" title="Total Memory: RSS reale (OS) quando disponibile e selezionato, altrimenti stima heap+overhead+off-heap. JVM Heap Max: Xmx stimato = (maxMemory / spark.memory.fraction) + 300MB.">Total Memory / Heap Max (MB)</th>
            <th class="heap-table-sortable" data-sort-key="heapPct" data-label="JVM Heap (MB) / Heap % (Xmx est.)" title="JVM Heap (MB) / Heap % of estimated Xmx (JVM heap / estimated Xmx). Xmx est. = (maxMemory / spark.memory.fraction) + 300MB. Heap % can exceed 100% when heap metric is a peak.">JVM Heap (MB) / Heap %</th>
            <th class="heap-table-sortable" data-sort-key="jvmOffHeapMB" data-label="Overhead / JVM Off-Heap (MB) / % of Total" title="Overhead (MB) e come percentuale della Total Memory della stessa riga.">Overhead / JVM Off-Heap (MB) / %</th>
            <th class="heap-table-sortable" data-sort-key="offHeapUnifiedMB" data-label="Spark Off-Heap (MB) / % of Total" title="Spark Off-Heap (MB) e come percentuale della Total Memory della stessa riga.">Spark Off-Heap (MB) / %</th>
            <th class="heap-table-sortable" data-sort-key="nativeOtherMB" data-label="Native/Other (MB) / % of Total" title="RSS reale − (Heap+Overhead+Spark Off-Heap): stack thread, buffer nativi/diretti (Netty, Snappy/Zstd, ...), overhead allocator nativi. NON include metaspace/code cache (già in Overhead). Disponibile solo quando Total Memory usa la fonte RSS. Percentuale calcolata sulla Total Memory della stessa riga.">Native/Other (MB) / %</th>
            <th class="heap-table-sortable" data-sort-key="gcSeconds" data-label="GC Time (s)">GC Time (s)</th>
            <th class="heap-table-sortable" data-sort-key="gcDeltaMs" data-label="GC ∆ last poll">GC ∆ last poll</th>
            <th class="heap-table-sortable" data-sort-key="activeTasks" data-label="Active Tasks">Active Tasks</th>
            <th class="heap-table-sortable" data-sort-key="maxTasks" data-label="Max Tasks">Max Tasks</th>
            <th class="heap-table-sortable" data-sort-key="saturationPct" data-label="Task Saturation %">Task Saturation %</th>
            <th class="heap-table-sortable" data-sort-key="cpuPct" data-label="CPU %" title="OS-level process CPU% (real, live) when the optional ProcessCpuPlugin is enabled via spark.plugins; otherwise delta of ex.cpuTimeMs (real, cumulative per-task CPU time) / (elapsed × numCores), falling back to a totalDuration-based estimate (badge shown) if cpuTimeMs is unavailable or no task has completed yet this poll. Available from the 2nd poll.">CPU % (util.)</th>
            <th class="heap-table-sortable" data-sort-key="failedTasks" data-label="Failed Tasks">Failed Tasks</th>
            <th class="heap-table-sortable" data-sort-key="shuffleReadMB" data-label="Shuffle Read (MB)">Shuffle Read (MB)</th>
            <th class="heap-table-sortable" data-sort-key="shuffleWriteMB" data-label="Shuffle Write (MB)">Shuffle Write (MB)</th>
            <th class="heap-table-sortable" data-sort-key="rddBlocks" data-label="RDD Blocks">RDD Blocks</th>
            <th class="heap-table-sortable" data-sort-key="diskUsedMB" data-label="Disk Used (MB)">Disk Used (MB)</th>
            <th class="heap-table-sortable" data-sort-key="memoryUsedMB" data-label="Storage Memory Used (MB)">Storage Memory Used (MB)</th>
            <th class="heap-table-sortable" data-sort-key="onHeapStorageMB" data-label="On-Heap Storage Used (MB)">On-Heap Storage Used (MB)</th>
            <th class="heap-table-sortable" data-sort-key="totalOnHeapCapacityMB" data-label="Total On-Heap Capacity (MB)">Total On-Heap Capacity (MB)</th>
            <th class="heap-table-sortable" data-sort-key="onHeapExecMB" data-label="On-Heap Exec (MB)">On-Heap Exec (MB)</th>
            <th class="heap-table-sortable" data-sort-key="onHeapStoragePeakMB" data-label="On-Heap Storage Peak (MB)">On-Heap Storage Peak (MB)</th>
            <th class="heap-table-sortable" data-sort-key="onHeapUnifiedMB" data-label="On-Heap Unified (MB)">On-Heap Unified (MB)</th>
          </tr>
        </thead>
        <tbody id="heapTableBody"></tbody>
      </table>

      <script>
        {Unparsed(buildScript(appId, metricsUrl, ChartsPage.PollIntervalSeconds, ChartsPage.MaxPoints, sparkMemoryFraction, reservedSystemMemoryBytes))}
      </script>
    </div>
  }

  /** Returns the plain JavaScript source embedded in the page. */
  private def buildScript(appId: String, metricsUrl: String,
                          pollSeconds: Int, maxPoints: Int,
                          sparkMemoryFraction: Double, reservedSystemMemoryBytes: Long): String =
    s"""
(function () {
  'use strict';

  function setFatalStatus(msg) {
    try {
      var status = document.getElementById('heapStatus');
      if (status) {
        status.textContent = msg;
        status.style.color = '#a94442';
      }
    } catch (ignored) {}
  }

  window.addEventListener('error', function(e) {
    var message = e && e.message ? e.message : String(e);
    setFatalStatus('Charts UI error: ' + message + ' (see browser console)');
  });
  window.addEventListener('unhandledrejection', function(e) {
    var reason = e && e.reason ? (e.reason.message || String(e.reason)) : 'unknown rejection';
    setFatalStatus('Charts UI async error: ' + reason + ' (see browser console)');
  });

  /* ── state ────────────────────────────────────────────────────── */
  var APP_ID = '$appId';
  var METRICS_URL = '$metricsUrl';
  var SPARK_MEMORY_FRACTION = $sparkMemoryFraction;
  var RESERVED_SYSTEM_MEMORY_BYTES = $reservedSystemMemoryBytes;
  var MAX_POINTS = $maxPoints;
  var pollMs     = $pollSeconds * 1000;
  var intervalId = null;

  var labels      = [];          // shared x-axis timestamps
  var executors   = {};          // id -> { label, heapData[], gcData[], saturationData[], color }
  var colorIdx    = 0;
  var latestExecutorPayload = [];
  var latestEndpoint = METRICS_URL;
  var latestRows = [];
  var latestVisibleRows = [];
  var latestWithHeap = 0;
  var latestRecordCount = 0;
  var sortState = { key: 'heapPct', direction: 'desc' };
  var prevTableValuesByExecutor = {}; // id -> last rendered numeric values (used for glow on changes)
  var selectedExecutors = {};  // id -> true means selected for charts when mode=selected
  var gcPrevState = {};   // id -> { gcMs, duration } — used to compute the latest GC delta
  var cpuPrevState = {};  // id -> { cpuMs, totalDuration, wallMs } — used to compute CPU delta
  var HISTORY_KEY = 'hpsc.heap.history.' + APP_ID;
  var HISTORY_MAX_ROWS = 20000;
  // On long-running applications, keeping unbounded (or only row-count-bounded) history in memory
  // makes historyRows/DOM/chart data grow for hours or days, which eventually makes the tab
  // sluggish or unresponsive in the browser. To keep memory usage predictable, samples older than
  // this age are dropped every time new rows are recorded (see trimHistoryByAge()), regardless of
  // how many rows that leaves. HISTORY_MAX_ROWS above remains as a secondary safety net for
  // pathological cases (e.g. extremely short poll interval with many executors).
  var HISTORY_MAX_AGE_MS = 60 * 60 * 1000; // 1 hour
  // Persistence to localStorage is a best-effort convenience (survive page reloads) and is
  // capped much smaller than HISTORY_MAX_ROWS to stay well within typical storage quotas. It must
  // NEVER be allowed to influence how much history is kept in memory: the in-memory historyRows
  // buffer (used to render "Last Nm"/"All history") is only ever bounded by HISTORY_MAX_ROWS and
  // HISTORY_MAX_AGE_MS.
  var PERSIST_MAX_ROWS = 500;
  var historyRows = [];
  var latestPollLabel = null;
  var pollSeq = 0;            // monotonic counter: one unique value per poll
  var lastRecordedSeq = -1;   // last pollSeq recorded in history (locale-independent)
  var localStorageAvailable = false;  // diagnostics: localStorage availability
  var LAYOUT_KEY = 'hpsc.heap.layout.' + APP_ID;
  // Real OS-level RSS (org.apache.spark.metrics.ProcessTreeMetrics, requires
  // spark.executor.processTreeMetrics.enabled=true) vs. the estimated total (heap + JVM off-heap +
  // Spark off-heap unified, from Spark's own internal counters). Persisted globally (not per-app)
  // since it is a display preference, not application data. Defaults to 'rss' and, as of this
  // version, NEVER silently falls back to the estimate when a sample lacks an RSS reading: enabling
  // spark.executor.processTreeMetrics.enabled=true is now a hard prerequisite for a complete "Total"
  // chart/column. If it isn't enabled, "Total" simply stays empty (a gap in the chart, a "-" in the
  // table) rather than being silently patched with the (potentially misleading) estimate — this is
  // intentional: it surfaces the missing configuration instead of masking it. Switch to 'estimated'
  // manually (see the "Total source" toggle) only for temporary debugging on clusters where enabling
  // process-tree metrics isn't possible.
  var TOTAL_MEMORY_MODE_KEY = 'hpsc.heap.totalMemoryMode';
  var totalMemoryMode = 'rss'; // 'rss' | 'estimated'
  var processTreeMetricsAvailable = false; // becomes true once any executor reports a non-null RSS sample
  var layoutState = {
    preset: 'default',
    heights: {
      memory: 650,
      cpu: 210,
      gc: 210,
      saturation: 210
    }
  };
  var viewRangeMinutes = null;  // null = current session, Number = last N minutes, Infinity = all
  var histViewLabels = [];
  var histViewExecutors = {};
  var histViewRows = [];
  var COLORS = [
    '#FF6384','#36A2EB','#FFCE56','#4BC0C0',
    '#9966FF','#FF9F40','#C9CBCF','#5C6BC0',
    '#26A69A','#EF5350','#8D6E63','#42A5F5'
  ];

  var canvas         = document.getElementById('heapChart');
  var chartContainer = document.getElementById('heapChartContainer');
  var chartsGridNode = document.getElementById('heapChartsGrid');
  var legendNode     = document.getElementById('heapLegend');
  var summaryNode    = document.getElementById('heapSummary');
  var topOffendersNode = document.getElementById('heapTopOffenders');
  var ctx            = canvas.getContext('2d');
  var gcCanvas       = document.getElementById('gcChart');
  var gcChartContainer = document.getElementById('gcChartContainer');
  var gcCtx          = gcCanvas.getContext('2d');
  var saturationCanvas = document.getElementById('saturationChart');
  var saturationChartContainer = document.getElementById('saturationChartContainer');
  var saturationCtx  = saturationCanvas.getContext('2d');
  var cpuCanvas = document.getElementById('cpuChart');
  var cpuChartContainer = document.getElementById('cpuChartContainer');
  var cpuCtx = cpuCanvas.getContext('2d');
  var tableHeaders   = Array.prototype.slice.call(document.querySelectorAll('#heapTable thead th[data-sort-key]'));

  if (!canvas || !gcCanvas || !saturationCanvas || !cpuCanvas || !summaryNode || !topOffendersNode || !chartsGridNode) {
    throw new Error('DOM not initialized: missing UI canvas/nodes');
  }
  setFatalStatus('Loading metrics...');

  /* ── helpers ───────────────────────────────────────────────────── */
  function toMB(bytes)  { return bytes / 1048576; }
  function toNum(v) {
    if (v == null) return null;
    var n = Number(v);
    return Number.isFinite(n) ? n : null;
  }
  function nowLabel()   {
    var d = new Date();
    return d.toLocaleTimeString([], { hour12: false });
  }
  function escapeHtml(value) {
    return String(value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }
  function colorWithAlpha(hex, alpha) {
    if (!hex) return 'rgba(120,120,120,' + alpha + ')';
    var value = String(hex).replace('#', '');
    if (value.length === 3) {
      value = value[0] + value[0] + value[1] + value[1] + value[2] + value[2];
    }
    var n = parseInt(value, 16);
    if (!Number.isFinite(n)) return 'rgba(120,120,120,' + alpha + ')';
    var r = (n >> 16) & 255;
    var g = (n >> 8) & 255;
    var b = n & 255;
    return 'rgba(' + r + ',' + g + ',' + b + ',' + alpha + ')';
  }
  function shadeColor(hex, factor) {
    var value = String(hex || '#888888').replace('#', '');
    if (value.length === 3) value = value[0] + value[0] + value[1] + value[1] + value[2] + value[2];
    var n = parseInt(value, 16);
    if (!Number.isFinite(n)) return '#888888';
    var r = (n >> 16) & 255;
    var g = (n >> 8) & 255;
    var b = n & 255;
    var apply = function(c) {
      if (factor >= 0) return Math.round(c + (255 - c) * factor);
      return Math.round(c * (1 + factor));
    };
    r = Math.max(0, Math.min(255, apply(r)));
    g = Math.max(0, Math.min(255, apply(g)));
    b = Math.max(0, Math.min(255, apply(b)));
    var toHex = function(v) { var h = v.toString(16); return h.length < 2 ? '0' + h : h; };
    return '#' + toHex(r) + toHex(g) + toHex(b);
  }
  function fmtInt(v) {
    return v == null ? '-' : String(Math.round(v));
  }
  function fmtMB(v) {
    return v == null ? '-' : v.toFixed(1);
  }
  function fmtBytesAsMB(bytes) {
    return bytes == null ? '-' : toMB(bytes).toFixed(1);
  }
  function fmtPct(v) {
    return v == null ? '-' : v.toFixed(1) + '%';
  }
  function pctOfTotal(valueMB, totalMB) {
    if (valueMB == null || totalMB == null || totalMB <= 0) return null;
    return (valueMB * 100.0) / totalMB;
  }
  function fmtSeconds(v) {
    return v == null ? '-' : v.toFixed(1);
  }
  function cardStyle(kind) {
    if (kind === 'danger') return 'color:#a94442;';
    if (kind === 'warn') return 'color:#8a6d3b;';
    return 'color:#333;';
  }
  function historyEnabled() {
    var cb = document.getElementById('recordHistory');
    return !!(cb && cb.checked);
  }
  function updateHistoryStatus() {
    var node = document.getElementById('historyStatus');
    if (!node) return;
    var storageStatus = localStorageAvailable ? '📦' : '⚠️ no storage';
    node.textContent = storageStatus + ' history: ' + historyRows.length + ' rows';
  }
  function toBoundedInt(v, dflt, min, max) {
    var n = parseInt(v, 10);
    if (!Number.isFinite(n)) n = dflt;
    return Math.max(min, Math.min(max, n));
  }
  function syncLayoutControlsFromState() {
    var preset = document.getElementById('layoutPreset');
    if (preset) preset.value = layoutState.preset;
    var mh = document.getElementById('memoryChartHeight');
    if (mh) mh.value = String(layoutState.heights.memory);
    var ch = document.getElementById('cpuChartHeight');
    if (ch) ch.value = String(layoutState.heights.cpu);
    var gh = document.getElementById('gcChartHeight');
    if (gh) gh.value = String(layoutState.heights.gc);
    var sh = document.getElementById('satChartHeight');
    if (sh) sh.value = String(layoutState.heights.saturation);
  }
  function applyLayoutState() {
    var allowed = {
      'default': true,
      'vertical': true,
      'focus-memory': true,
      'focus-cpu': true,
      'focus-gc': true,
      'focus-saturation': true
    };
    if (!allowed[layoutState.preset]) layoutState.preset = 'default';
    var cls = 'heap-charts-grid layout-' + layoutState.preset;
    chartsGridNode.className = cls;
    syncLayoutControlsFromState();
  }
  function saveLayoutState() {
    try {
      if (!window.sessionStorage) return;
      sessionStorage.setItem(LAYOUT_KEY, JSON.stringify(layoutState));
    } catch (e) {
      console.warn('[Charts] sessionStorage layout not available:', e.message);
    }
  }
  function loadLayoutState() {
    try {
      if (!window.sessionStorage) return;
      var raw = sessionStorage.getItem(LAYOUT_KEY);
      if (!raw) return;
      var parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== 'object') return;
      layoutState.preset = parsed.preset || layoutState.preset;
      var h = parsed.heights || {};
      layoutState.heights.memory = toBoundedInt(h.memory, layoutState.heights.memory, 180, 900);
      layoutState.heights.cpu = toBoundedInt(h.cpu, layoutState.heights.cpu, 180, 900);
      layoutState.heights.gc = toBoundedInt(h.gc, layoutState.heights.gc, 140, 700);
      layoutState.heights.saturation = toBoundedInt(h.saturation, layoutState.heights.saturation, 140, 700);
    } catch (e) {
      console.warn('[Charts] Unable to read saved layout:', e.message);
    }
  }
  window.applyLayoutControls = function() {
    var preset = document.getElementById('layoutPreset');
    var mh = document.getElementById('memoryChartHeight');
    var ch = document.getElementById('cpuChartHeight');
    var gh = document.getElementById('gcChartHeight');
    var sh = document.getElementById('satChartHeight');
    layoutState.preset = preset ? preset.value : layoutState.preset;
    layoutState.heights.memory = toBoundedInt(mh ? mh.value : layoutState.heights.memory, 650, 180, 900);
    layoutState.heights.cpu = toBoundedInt(ch ? ch.value : layoutState.heights.cpu, 210, 180, 900);
    layoutState.heights.gc = toBoundedInt(gh ? gh.value : layoutState.heights.gc, 210, 140, 700);
    layoutState.heights.saturation = toBoundedInt(sh ? sh.value : layoutState.heights.saturation, 210, 140, 700);
    applyLayoutState();
    saveLayoutState();
    renderCharts(latestVisibleRows || []);
  };
  window.resetLayoutControls = function() {
    layoutState = {
      preset: 'default',
      heights: { memory: 650, cpu: 210, gc: 210, saturation: 210 }
    };
    applyLayoutState();
    saveLayoutState();
    renderCharts(latestVisibleRows || []);
  };
  var HISTORY_KEY_PREFIX = 'hpsc.heap.history.';
  /**
   * Removes leftover `hpsc.heap.history.<applicationId>` entries left behind by *other*
   * (typically already-finished) Spark applications previously monitored from this browser
   * origin. Each entry uses a unique, never-reused key (the application ID), so without this
   * cleanup they accumulate forever and eventually exhaust the origin's whole localStorage quota
   * — causing `saveHistory()` to fail with a genuine `QuotaExceededError` on the very first write
   * of a brand-new application, even though that new write is tiny on its own. This is a common
   * scenario on long-lived, shared browser sessions (e.g. an analyst's browser kept open for
   * weeks, reused for many Spark applications through the same YARN ResourceManager/Knox proxy
   * URL). Only entries for *other* application IDs are removed; the current one is left untouched.
   */
  function purgeStaleHistoryEntries() {
    var staleKeys = [];
    for (var i = 0; i < localStorage.length; i++) {
      var key = localStorage.key(i);
      if (key && key.indexOf(HISTORY_KEY_PREFIX) === 0 && key !== HISTORY_KEY) {
        staleKeys.push(key);
      }
    }
    staleKeys.forEach(function(key) {
      try { localStorage.removeItem(key); } catch (ignored) {}
    });
  }
  function loadHistory() {
    try {
      if (!window.localStorage) throw new Error('localStorage not available');
      try { purgeStaleHistoryEntries(); } catch (ignored) {}
      var raw = localStorage.getItem(HISTORY_KEY);
      historyRows = raw ? JSON.parse(raw) : [];
      if (!Array.isArray(historyRows)) historyRows = [];
      trimHistoryByAge(); // discard anything older than 1h restored from a stale persisted snapshot
      localStorageAvailable = true;
    } catch (e) {
      historyRows = [];
      localStorageAvailable = false;
      console.warn('[Charts] localStorage not available:', e.message);
    }
    updateHistoryStatus();
  }
  /**
   * Persists (a bounded tail of) `historyRows` to localStorage, purely as a best-effort
   * convenience so history survives a page reload. This must NEVER mutate the in-memory
   * `historyRows` buffer itself — that buffer is the single source of truth rendered by the
   * "Last Nm"/"All history" views and is only ever bounded by HISTORY_MAX_ROWS. If persistence
   * fails for any reason (quota exceeded, storage blocked/partitioned behind a reverse proxy such
   * as the YARN ResourceManager/Knox app proxy, private-browsing restrictions, etc.) we simply
   * stop attempting to persist for the rest of this session; in-memory recording keeps working
   * regardless, so charts/table/CSV export are unaffected — only "persist across reloads" is lost.
   */
  function saveHistory() {
    if (!localStorageAvailable) { updateHistoryStatus(); return; }
    try {
      var toPersist = historyRows.length > PERSIST_MAX_ROWS
        ? historyRows.slice(historyRows.length - PERSIST_MAX_ROWS)
        : historyRows;
      localStorage.setItem(HISTORY_KEY, JSON.stringify(toPersist));
    } catch (e) {
      localStorageAvailable = false;
      console.warn('[Charts] localStorage write failed, disabling persistence for this session ' +
        '(history is kept in memory only, unaffected by this):', e.message);
    }
    updateHistoryStatus();
  }
  function recordRowsSnapshot(rows) {
    if (!historyEnabled()) return;
    if (pollSeq === 0 || lastRecordedSeq === pollSeq) return;   // not started yet or already recorded
    var nowIso = new Date().toISOString();
    rows.forEach(function(row) {
      historyRows.push({
        appId: APP_ID,
        timeLabel: latestPollLabel,
        timeIso: nowIso,
        executor: row.id,
        host: row.host,
        heapMB: row.heapMB,
        heapMaxMB: row.heapMaxMB,
        heapPct: row.heapPct,
        jvmOffHeapMB: row.jvmOffHeapMB,
        offHeapUnifiedMB: row.offHeapUnifiedMB,
        rssMB: row.rssMB,
        totalMemoryMB: row.totalMemoryMB != null ? row.totalMemoryMB : totalMemoryMBOf(row.heapMB, row.jvmOffHeapMB, row.offHeapUnifiedMB),
        totalMemorySource: row.totalMemorySource,
        nativeOtherMB: row.nativeOtherMB,
        gcTimeSeconds: row.gcSeconds,
        gcDeltaMs: row.gcDeltaMs,
        activeTasks: row.activeTasks,
        maxTasks: row.maxTasks,
        saturationPct: row.saturationPct,
        cpuPct: row.cpuPct,
        failedTasks: row.failedTasks,
        shuffleReadMB: row.shuffleReadMB,
        shuffleWriteMB: row.shuffleWriteMB,
        rddBlocks: row.rddBlocks,
        diskUsedMB: row.diskUsedMB,
        memoryUsedMB: row.memoryUsedMB,
        onHeapStorageMB: row.onHeapStorageMB,
        totalOnHeapCapacityMB: row.totalOnHeapCapacityMB,
        onHeapExecMB: row.onHeapExecMB,
        onHeapStoragePeakMB: row.onHeapStoragePeakMB,
        onHeapUnifiedMB: row.onHeapUnifiedMB,
        metricSource: row.metricSource
      });
    });
    trimHistoryByAge();
    if (historyRows.length > HISTORY_MAX_ROWS) {
      historyRows = historyRows.slice(historyRows.length - HISTORY_MAX_ROWS);
    }
    lastRecordedSeq = pollSeq;
    saveHistory();
  }
  /**
   * Drops in-memory history samples older than HISTORY_MAX_AGE_MS. This is the primary bound on
   * memory usage for long-running applications: without it, historyRows (and the DOM/chart data
   * derived from it) would keep growing for as long as the browser tab stays open, eventually
   * making Chrome/the page sluggish or unresponsive. Rows with an unparsable timeIso are kept
   * (fail open) rather than risk silently discarding valid data.
   */
  function trimHistoryByAge() {
    var cutoffMs = Date.now() - HISTORY_MAX_AGE_MS;
    historyRows = historyRows.filter(function(r) {
      var t = new Date(r.timeIso).getTime();
      return isNaN(t) || t >= cutoffMs;
    });
  }
  function csvValue(v) {
    if (v == null) return '';
    var s = String(v);
    var NEWLINE_CHAR = String.fromCharCode(10);
    var needsQuote = s.indexOf('"') >= 0 || s.indexOf(NEWLINE_CHAR) >= 0 || s.indexOf(';') >= 0 || s.indexOf(',') >= 0;
    if (needsQuote) return '"' + s.split('"').join('""') + '"';
    return s;
  }
  function downloadHistoryCsv() {
    if (historyRows.length === 0) return;
    var headers = [
      'appId','timeLabel','timeIso','executor','host','heapMB','heapMaxMB','heapPct',
      'gcTimeSeconds','gcDeltaMs','activeTasks','maxTasks','saturationPct','cpuPct','failedTasks',
      'jvmOffHeapMB','offHeapUnifiedMB','rssMB','totalMemoryMB','totalMemorySource','nativeOtherMB','onHeapExecMB','onHeapStoragePeakMB','onHeapUnifiedMB',
      'onHeapStorageMB','totalOnHeapCapacityMB','rddBlocks','diskUsedMB','memoryUsedMB',
      'shuffleReadMB','shuffleWriteMB','metricSource'
    ];
    var lines = [headers.join(';')];
    historyRows.forEach(function(r) {
      lines.push(headers.map(function(h) { return csvValue(r[h]); }).join(';'));
    });
    var blob = new Blob([lines.join(String.fromCharCode(10))], { type: 'text/csv;charset=utf-8;' });
    var link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = 'heap-history-' + APP_ID + '.csv';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(link.href);
  }
  function resetHistory() {
    historyRows = [];
    lastRecordedSeq = -1;
    try { if (window.localStorage) localStorage.removeItem(HISTORY_KEY); } catch (e) {}
    updateHistoryStatus();
  }
  function buildHistoryView(minutes) {
    var cutoffMs = (minutes == null || !isFinite(minutes)) ? null : (Date.now() - minutes * 60000);
    var filtered = historyRows.filter(function(r) {
      if (cutoffMs == null) return true;
      try { return new Date(r.timeIso).getTime() >= cutoffMs; } catch(e) { return true; }
    });
    var infoNode = document.getElementById('histViewInfo');
    if (filtered.length === 0) {
      histViewLabels = [];
      histViewExecutors = {};
      histViewRows = [];
      if (infoNode) infoNode.textContent = 'No historical data for this range.';
      return;
    }
    // Build ordered time axis
    var timeMap = {};
    filtered.forEach(function(r) { if (!timeMap[r.timeIso]) timeMap[r.timeIso] = r.timeLabel; });
    var sortedTimes = Object.keys(timeMap).sort();
    var timeIndex = {};
    sortedTimes.forEach(function(t, i) { timeIndex[t] = i; });
    histViewLabels = sortedTimes.map(function(t) { return timeMap[t]; });
    var n = sortedTimes.length;
    // Build executor series
    histViewExecutors = {};
    filtered.forEach(function(r) {
      var id = r.executor;
      if (!histViewExecutors[id]) {
        var color = (executors[id] && executors[id].color) ? executors[id].color : COLORS[colorIdx++ % COLORS.length];
        histViewExecutors[id] = {
          label: 'Executor ' + id,
          heapData: new Array(n).fill(null),
          overheadData: new Array(n).fill(null),
          offHeapData: new Array(n).fill(null),
          totalMemoryData: new Array(n).fill(null),
          rssData: new Array(n).fill(null),
          gcData: new Array(n).fill(null),
          saturationData: new Array(n).fill(null),
          cpuData: new Array(n).fill(null),
          color: color
        };
      }
      var idx = timeIndex[r.timeIso];
      if (r.heapMB != null) histViewExecutors[id].heapData[idx] = r.heapMB;
      if (r.jvmOffHeapMB != null) histViewExecutors[id].overheadData[idx] = r.jvmOffHeapMB;
      if (r.offHeapUnifiedMB != null) histViewExecutors[id].offHeapData[idx] = r.offHeapUnifiedMB;
      var histTotalMemoryMB = r.totalMemoryMB != null ? r.totalMemoryMB : totalMemoryMBOf(r.heapMB, r.jvmOffHeapMB, r.offHeapUnifiedMB);
      if (histTotalMemoryMB != null) histViewExecutors[id].totalMemoryData[idx] = histTotalMemoryMB;
      if (r.rssMB != null) histViewExecutors[id].rssData[idx] = r.rssMB;
      if (r.gcDeltaMs != null) histViewExecutors[id].gcData[idx] = r.gcDeltaMs;
      if (r.saturationPct != null) histViewExecutors[id].saturationData[idx] = r.saturationPct;
      if (r.cpuPct != null) histViewExecutors[id].cpuData[idx] = r.cpuPct;
    });
    // Build histViewRows: last sample per executor in range
    var latestPer = {};
    filtered.forEach(function(r) {
      if (!latestPer[r.executor] || r.timeIso > latestPer[r.executor].timeIso) latestPer[r.executor] = r;
    });
    var warnPctRaw = toNum(document.getElementById('warnPct').value);
    var warnPct = warnPctRaw != null ? warnPctRaw : 85;
    var satWarnPctRaw = toNum(document.getElementById('satWarnPct').value);
    var satWarnPct = satWarnPctRaw != null ? satWarnPctRaw : 400;
    histViewRows = Object.keys(latestPer).map(function(id) {
      var r = latestPer[id];
      var resolvedHistTotal = resolveTotalMemory(r.heapMB, r.jvmOffHeapMB, r.offHeapUnifiedMB, r.rssMB);
      var row = {
        id: id, host: r.host,
        heapB: r.heapMB != null ? r.heapMB * 1048576 : null,
        heapMB: r.heapMB, heapMaxB: r.heapMaxMB != null ? r.heapMaxMB * 1048576 : 0,
        heapMaxMB: r.heapMaxMB,
        heapPct: (r.heapMB != null && r.heapMaxMB != null && r.heapMaxMB > 0) ? ((r.heapMB * 100.0) / r.heapMaxMB) : null,
        jvmOffHeapMB: r.jvmOffHeapMB,
        offHeapUnifiedMB: r.offHeapUnifiedMB,
        rssMB: r.rssMB,
        totalMemoryMB: resolvedHistTotal.value,
        totalMemorySource: resolvedHistTotal.source,
        nativeOtherMB: nativeOtherMBOf(r.heapMB, r.jvmOffHeapMB, r.offHeapUnifiedMB, resolvedHistTotal.value, resolvedHistTotal.source),
        gcSeconds: r.gcTimeSeconds, gcPct: null,
        gcDeltaMs: r.gcDeltaMs, activeTasks: r.activeTasks, maxTasks: r.maxTasks,
        saturationPct: r.saturationPct, cpuPct: r.cpuPct, failedTasks: r.failedTasks,
        shuffleReadMB: r.shuffleReadMB, shuffleWriteMB: r.shuffleWriteMB,
        rddBlocks: r.rddBlocks, diskUsedMB: r.diskUsedMB, memoryUsedMB: r.memoryUsedMB,
        onHeapStorageMB: r.onHeapStorageMB, totalOnHeapCapacityMB: r.totalOnHeapCapacityMB,
        onHeapExecMB: r.onHeapExecMB,
        onHeapStoragePeakMB: r.onHeapStoragePeakMB, onHeapUnifiedMB: r.onHeapUnifiedMB,
        metricSource: r.metricSource
      };
      row.isUnhealthy = isRowUnhealthy(row, warnPct, satWarnPct);
      return row;
    });
    if (infoNode) {
      var label = isFinite(minutes) ? ('last ' + minutes + 'm') : 'full history';
      infoNode.textContent = label + ': ' + n + ' samples, ' + histViewRows.length + ' executors' +
        ' | total history: ' + historyRows.length + ' rows (pollSeq=' + pollSeq + ')';
    }
  }
  function updateViewRangeButtons() {
    var buttons = document.querySelectorAll('.heap-vr-btn');
    Array.prototype.forEach.call(buttons, function(btn) {
      var vr = btn.getAttribute('data-vr');
      var isActive;
      if (vr === 'session') isActive = (viewRangeMinutes === null);
      else if (vr === 'all') isActive = (viewRangeMinutes === Infinity);
      else isActive = (viewRangeMinutes === Number(vr));
      btn.className = isActive ? 'btn btn-xs btn-primary heap-vr-btn' : 'btn btn-xs btn-default heap-vr-btn';
    });
    var infoNode = document.getElementById('histViewInfo');
    if (viewRangeMinutes === null && infoNode) infoNode.textContent = '';
  }
  function setViewRange(minutes) {
    viewRangeMinutes = minutes;
    if (minutes !== null) buildHistoryView(minutes);
    updateViewRangeButtons();
    rerenderFromState();
  }
  function niceCeiling(maxValue) {
    if (!Number.isFinite(maxValue) || maxValue <= 0) return 1;
    var exponent = Math.floor(Math.log(maxValue) / Math.LN10);
    var fraction = maxValue / Math.pow(10, exponent);
    // Finer-grained steps than the classic 1/2/5/10 sequence: with only those, a value just above
    // a power of ten (e.g. 1100) would round all the way up to the next one (2000), giving ~2x
    // headroom instead of the ~10-20% actually needed to keep the line readable near the top.
    var steps = [1, 1.2, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10];
    var niceFraction = 10;
    for (var i = 0; i < steps.length; i += 1) {
      if (fraction <= steps[i]) {
        niceFraction = steps[i];
        break;
      }
    }
    return niceFraction * Math.pow(10, exponent);
  }
  function syncCanvasSize(canvasNode, containerNode, height) {
    var width = Math.max(containerNode.clientWidth - 16, 260);
    var dpr = window.devicePixelRatio || 1;
    canvasNode.style.width = width + 'px';
    canvasNode.style.height = height + 'px';
    canvasNode.width = Math.floor(width * dpr);
    canvasNode.height = Math.floor(height * dpr);
    return { width: width, height: height };
  }
  function latestValue(series) {
    if (!Array.isArray(series)) return null;
    for (var i = series.length - 1; i >= 0; i -= 1) {
      if (series[i] != null) return series[i];
    }
    return null;
  }
  function totalMemoryMBOf(heapMB, overheadMB, offHeapMB) {
    var total = 0;
    var hasAny = false;
    [heapMB, overheadMB, offHeapMB].forEach(function(v) {
      if (v != null) {
        total += v;
        hasAny = true;
      }
    });
    return hasAny ? total : null;
  }
  /**
   * "Native/Other" gap: the part of the real RSS total not accounted for by Spark's own
   * heap+overhead+off-heap counters. Note that Metaspace/Code Cache/Compressed Class Space are
   * already included in "overhead" (JVMOffHeapMemory = MemoryMXBean.getNonHeapMemoryUsage()), so
   * this gap instead covers what NO JVM/Spark counter tracks: thread stacks, native/direct buffers
   * (Netty, Snappy/Zstd, Arrow, JNI), and native allocator overhead (e.g. glibc malloc arenas).
   * Only meaningful when the Total Memory shown is sourced from RSS (see resolveTotalMemory) — for
   * the "estimated" source the stack sums exactly to the total by construction, so the gap is
   * always 0/undefined there.
   */
  function nativeOtherMBOf(heapMB, overheadMB, offHeapMB, totalMemoryMB, totalMemorySource) {
    if (totalMemorySource !== 'rss' || totalMemoryMB == null) return null;
    var known = (heapMB || 0) + (overheadMB || 0) + (offHeapMB || 0);
    var gap = totalMemoryMB - known;
    return gap > 0 ? gap : 0;
  }
  /**
   * Resolves the "Total Memory" value to display, choosing between the real OS-level process-tree
   * RSS (ProcessTreeJVMRSSMemory, requires spark.executor.processTreeMetrics.enabled=true) and the
   * estimated sum of Spark's own internal counters (heap + JVM off-heap + Spark off-heap unified),
   * depending on the user-selected totalMemoryMode. Deliberately does NOT fall back to the estimate
   * when RSS mode is selected but this particular sample lacks an RSS reading: enabling
   * spark.executor.processTreeMetrics.enabled=true is a hard prerequisite for a complete "Total" in
   * RSS mode, and silently substituting the estimate would mask that the config isn't active (or
   * that a sample was transiently missed) instead of surfacing it as a visible gap/"-".
   */
  function resolveTotalMemory(heapMB, overheadMB, offHeapMB, rssMB) {
    if (totalMemoryMode === 'rss') return { value: rssMB, source: rssMB != null ? 'rss' : null };
    return { value: totalMemoryMBOf(heapMB, overheadMB, offHeapMB), source: 'estimated' };
  }
  function loadTotalMemoryMode() {
    try {
      if (!window.localStorage) return;
      var saved = localStorage.getItem(TOTAL_MEMORY_MODE_KEY);
      if (saved === 'rss' || saved === 'estimated') totalMemoryMode = saved;
    } catch (e) {
      console.warn('[Charts] Unable to read saved totalMemoryMode:', e.message);
    }
  }
  function saveTotalMemoryMode() {
    try {
      if (window.localStorage) localStorage.setItem(TOTAL_MEMORY_MODE_KEY, totalMemoryMode);
    } catch (e) {
      console.warn('[Charts] Unable to persist totalMemoryMode:', e.message);
    }
  }
  window.setTotalMemoryMode = function(mode) {
    if (mode !== 'rss' && mode !== 'estimated') return;
    totalMemoryMode = mode;
    saveTotalMemoryMode();
    updateRssAvailabilityWarning();
    rerenderFromState();
  };
  /**
   * Keeps the "no RSS samples received yet" warning banner in sync: shown whenever RSS mode is
   * selected (the default) but no executor has reported a process-tree RSS sample yet, since
   * spark.executor.processTreeMetrics.enabled=true is now required for a complete "Total"
   * chart/column rather than being silently patched over with the estimate.
   */
  function updateRssAvailabilityWarning() {
    var warning = document.getElementById('rssMissingWarning');
    if (!warning) return;
    warning.style.display = (totalMemoryMode === 'rss' && !processTreeMetricsAvailable) ? 'block' : 'none';
  }
  function getHeapMetricSelection() {
    function checked(id) {
      var node = document.getElementById(id);
      return !!(node && node.checked);
    }
    var selection = {
      heap: checked('heapMetricHeap'),
      overhead: checked('heapMetricOverhead'),
      offHeap: checked('heapMetricOffHeap'),
      total: checked('heapMetricTotal'),
      nativeOther: checked('heapMetricNativeOther')
    };
    if (!selection.heap && !selection.overhead && !selection.offHeap && !selection.total && !selection.nativeOther) {
      selection.heap = true;
    }
    return selection;
  }
  /**
   * Heap reading for chart and Heap %.
   * Priority (aligned with spark-monitor, which uses peakMemoryMetrics):
   *   1. peakMemoryMetrics.JVMHeapMemory — standard source in CDP/Cloudera environments.
   *      In these environments executorMetrics.JVMHeapMemory is typically 0 or absent.
   *   2. executorMetrics.JVMHeapMemory — fallback for non-CDP environments.
   */
  function heapCurrentOf(ex) {
    // Primary source: peakMemoryMetrics
    var fromPeak = ex.peakMemoryMetrics && toNum(ex.peakMemoryMetrics.JVMHeapMemory);
    if (fromPeak != null && fromPeak > 0) return { bytes: fromPeak, source: 'peakMemoryMetrics.JVMHeapMemory', isPeak: false };

    // Fallback for non-CDP environments where executorMetrics is populated
    var fromExecutorMetrics = ex.executorMetrics && toNum(ex.executorMetrics.JVMHeapMemory);
    if (fromExecutorMetrics != null && fromExecutorMetrics > 0) return { bytes: fromExecutorMetrics, source: 'executorMetrics.JVMHeapMemory', isPeak: false };

    return { bytes: null, source: '-', isPeak: false };
  }
  /** Maximum reached value (monotonic increasing) — used only for Peak Heap table column. */
  function peakHeapOf(ex) {
    return ex.peakMemoryMetrics ? toNum(ex.peakMemoryMetrics.JVMHeapMemory) : null;
  }
  /**
   * Keeps heapReadingOf for backward compatibility — used when we want the best
   * available estimate regardless of type (current or peak).
   */
  function heapReadingOf(ex) {
    var fromExecutorMetrics = ex.executorMetrics && toNum(ex.executorMetrics.JVMHeapMemory);
    if (fromExecutorMetrics != null) return { bytes: fromExecutorMetrics, source: 'executorMetrics.JVMHeapMemory' };
    var fromPeak = ex.peakMemoryMetrics && toNum(ex.peakMemoryMetrics.JVMHeapMemory);
    if (fromPeak != null) return { bytes: fromPeak, source: 'peakMemoryMetrics.JVMHeapMemory (peak — monotono)' };
    var fromStorage = ex.memoryMetrics && toNum(ex.memoryMetrics.usedOnHeapStorageMemory);
    if (fromStorage != null) return { bytes: fromStorage, source: 'memoryMetrics.usedOnHeapStorageMemory' };
    return { bytes: null, source: '-' };
  }
  /**
   * GC delta in ms: GC time consumed in the interval between two consecutive polls.
   * Shows instantaneous GC pressure, not cumulative GC time.
   * Returns null on first poll (no previous sample available).
   */
  function gcDeltaMsCompute(id, curGcMs) {
    if (curGcMs == null) return null;
    var prev = gcPrevState[id];
    if (prev == null) return null;
    var gcDelta = curGcMs - prev.gcMs;
    return gcDelta >= 0 ? gcDelta : null;
  }
  function fmtGcDelta(ms) {
    if (ms == null) return '<span style="color:#aaa;font-style:italic;">wait…</span>';
    if (ms < 1000) return ms.toFixed(0) + ' ms';
    return (ms / 1000).toFixed(2) + ' s';
  }

  function heapMaxOf(ex) {
    var sparkManagedMax = toNum(ex.maxMemory);
    if (sparkManagedMax == null || sparkManagedMax <= 0) return 0;
    if (SPARK_MEMORY_FRACTION > 0 && SPARK_MEMORY_FRACTION < 1) {
      // ex.maxMemory in Spark REST is UnifiedMemory (storage/execution), not JVM Xmx.
      return (sparkManagedMax / SPARK_MEMORY_FRACTION) + RESERVED_SYSTEM_MEMORY_BYTES;
    }
    return sparkManagedMax;
  }
  function taskCapacityOf(ex) {
    var maxTasks = toNum(ex.maxTasks);
    if (maxTasks != null && maxTasks > 0) return maxTasks;
    var totalCores = toNum(ex.totalCores);
    return totalCores != null && totalCores > 0 ? totalCores : null;
  }
  function gcSecondsOf(ex) {
    var gcMs = toNum(ex.totalGCTime);
    return gcMs != null ? (gcMs / 1000.0) : null;
  }
  function gcPctOf(ex) {
    var gcMs = toNum(ex.totalGCTime);
    var totalDuration = toNum(ex.totalDuration);
    if (gcMs == null || totalDuration == null || totalDuration <= 0) return null;
    return (gcMs * 100.0) / totalDuration;
  }
  function saturationPctOf(ex) {
    var activeTasks = toNum(ex.activeTasks);
    var capacity = taskCapacityOf(ex);
    if (activeTasks == null || capacity == null || capacity <= 0) return null;
    return (activeTasks * 100.0) / capacity;
  }
  /**
   * CPU utilization % computed with a triple fallback:
   *  1. ex.osCpuLoadPct (real, live) — OS-level CPU% of the whole executor process, read directly
   *     from its PID via com.sun.management.OperatingSystemMXBean and computed server-side by the
   *     optional ProcessCpuPlugin (see its scaladoc). Unlike the other two sources below, this
   *     reflects CPU consumed by still-running tasks too, not just completed ones. Only present
   *     when the plugin is explicitly enabled via spark.plugins.
   *  2. ex.cpuTimeMs (real, ms) — cumulative per-task CPU time (ThreadMXBean-based, same
   *     measurement as Spark's own ExecutorSource "cpuTime" counter), aggregated server-side per
   *     executor by ExecutorCpuTimeListener and merged into the JSON payload by
   *     ChartsPage.renderJson — see that class's scaladoc for why this exists as a side-channel
   *     rather than a plain ExecutorSummary field.
   *  3. totalDuration delta (ms) — task wall-clock time vs available core-time; an ESTIMATE that
   *     assumes tasks are 100%-CPU-bound (no I/O/shuffle wait), used if cpuTimeMs is missing
   *     (e.g. a rolling upgrade briefly mixing an older backend build without the listener), and
   *     also as a live proxy while cpuTimeMs cannot yet reflect reality: Spark only updates a
   *     task's executorCpuTime once, when the task finishes (there is no incremental accounting
   *     while it's still running), so a 0 delta is ambiguous between "truly idle" and "task(s)
   *     still running, not finished yet this poll" - we only accept it as real when there are no
   *     active tasks, otherwise we fall through to this estimate so long-running tasks don't make
   *     CPU% appear stuck at 0 between task completions.
   * Returns null on first poll. Exposes source in series.lastCpuSource.
   */
  function cpuDeltaCompute(id, ex, curWallMs) {
    // Primary (real, live): OS-level process CPU%, already computed server-side - no delta/prev
    // state needed here, and it doesn't suffer from the task-completion-only granularity of the
    // other two sources below.
    var osCpuPct = toNum(ex.osCpuLoadPct);
    if (osCpuPct != null) return { pct: osCpuPct, source: 'OS' };

    var numCores = taskCapacityOf(ex);
    if (numCores == null || numCores <= 0) return { pct: null, source: '-' };
    var prev = cpuPrevState[id];
    if (prev == null) return { pct: null, source: 'wait' };
    var elapsedMs = curWallMs - prev.wallMs;
    if (elapsedMs <= 100) return { pct: null, source: 'wait' };  // too short

    // Secondary (real): cumulative real CPU time consumed by this executor's completed tasks.
    // NOTE: Spark only updates a task's executorCpuTime once, when the task *finishes* (there is
    // no live/incremental CPU accounting while a task is still running - this holds for both this
    // listener and Spark's own ExecutorSource "cpuTime" counter). So a delta of exactly 0 is
    // ambiguous: it can mean the executor is genuinely idle, or that task(s) are still busy
    // running but haven't completed during this poll interval yet. We only trust a 0 delta as a
    // real "idle" reading when there are no active tasks; otherwise we fall through to the
    // totalDuration estimate below so a busy executor running long tasks doesn't show a
    // misleading flat 0% for many consecutive polls.
    var curCpuMs = toNum(ex.cpuTimeMs);
    var activeTasks = toNum(ex.activeTasks) || 0;
    if (curCpuMs != null && prev.cpuMs != null) {
      var deltaCpuMs = curCpuMs - prev.cpuMs;
      if (deltaCpuMs > 0 || (deltaCpuMs === 0 && activeTasks === 0)) {
        return { pct: deltaCpuMs / (elapsedMs * numCores) * 100.0, source: 'TaskCpuTime' };
      }
    }

    // Fallback (estimate): totalDuration delta (task wall-clock time vs available core-time)
    var curDuration = toNum(ex.totalDuration);
    if (curDuration != null && prev.totalDuration != null) {
      var deltaDuration = curDuration - prev.totalDuration;
      if (deltaDuration >= 0) {
        return { pct: deltaDuration / (elapsedMs * numCores) * 100.0, source: 'totalDuration' };
      }
    }

    return { pct: null, source: '-' };
  }
  function ensureExecutorSeries(id) {
    if (!executors[id]) {
      var color = COLORS[colorIdx++ % COLORS.length];
      var heapSeries = [];
      var overheadSeries = [];
      var offHeapSeries = [];
      var totalMemorySeries = [];
      var rssSeries = [];
      var gcSeries = [];
      var saturationSeries = [];
      var cpuSeries = [];
      for (var i = 0; i < labels.length; i += 1) {
        heapSeries.push(null);
        overheadSeries.push(null);
        offHeapSeries.push(null);
        totalMemorySeries.push(null);
        rssSeries.push(null);
        gcSeries.push(null);
        saturationSeries.push(null);
        cpuSeries.push(null);
      }
      executors[id] = {
        label: 'Executor ' + id,
        heapData: heapSeries,
        overheadData: overheadSeries,
        offHeapData: offHeapSeries,
        totalMemoryData: totalMemorySeries,
        rssData: rssSeries,
        gcData: gcSeries,
        saturationData: saturationSeries,
        cpuData: cpuSeries,
        color: color
      };
    }
    while (executors[id].heapData.length < labels.length) executors[id].heapData.push(null);
    if (!Array.isArray(executors[id].overheadData)) executors[id].overheadData = [];
    while (executors[id].overheadData.length < labels.length) executors[id].overheadData.push(null);
    if (!Array.isArray(executors[id].offHeapData)) executors[id].offHeapData = [];
    while (executors[id].offHeapData.length < labels.length) executors[id].offHeapData.push(null);
    if (!Array.isArray(executors[id].totalMemoryData)) executors[id].totalMemoryData = [];
    while (executors[id].totalMemoryData.length < labels.length) executors[id].totalMemoryData.push(null);
    if (!Array.isArray(executors[id].rssData)) executors[id].rssData = [];
    while (executors[id].rssData.length < labels.length) executors[id].rssData.push(null);
    while (executors[id].gcData.length < labels.length) executors[id].gcData.push(null);
    while (executors[id].saturationData.length < labels.length) executors[id].saturationData.push(null);
    // cpuData may be missing for executors created before this metric was introduced
    if (!Array.isArray(executors[id].cpuData)) executors[id].cpuData = [];
    while (executors[id].cpuData.length < labels.length) executors[id].cpuData.push(null);
    return executors[id];
  }
  function visibleSeries(metricKey, allowedIds) {
    var allowed = null;
    if (allowedIds != null) {
      allowed = {};
      allowedIds.forEach(function(id) { allowed[id] = true; });
    }
    return Object.keys(executors)
      .sort(function(a, b) {
        if (a === 'driver') return -1;
        if (b === 'driver') return 1;
        return a.localeCompare(b, undefined, { numeric: true, sensitivity: 'base' });
      })
      .map(function(id) {
        var data = executors[id][metricKey];
        // Defensive: series may be missing if executor was created before a new metric was added
        if (!Array.isArray(data)) data = [];
        return { id: id, series: executors[id], data: data };
      })
      .filter(function(item) {
        return (!allowed || allowed[item.id]) && item.data.some(function(v) { return v != null; });
      });
  }
  function isMonitorSelected(id) {
    return !!selectedExecutors[id];
  }
  function getMonitorMode() {
    var selectedMode = document.getElementById('monitorModeSelected');
    return selectedMode && selectedMode.checked ? 'selected' : 'all';
  }
  function applyMonitorSelection(rows) {
    if (getMonitorMode() !== 'selected') return rows;
    return rows.filter(function(row) { return isMonitorSelected(row.id); });
  }
  function updateSelectedExecInfo(rows) {
    var node = document.getElementById('selectedExecInfo');
    if (!node) return;
    var selectedCount = Object.keys(selectedExecutors).filter(function(id) { return selectedExecutors[id]; }).length;
    var visibleCount = rows ? rows.length : 0;
    node.textContent = 'selected: ' + selectedCount + ' | visible: ' + visibleCount;
    var headerCb = document.getElementById('monitorAllCheckbox');
    if (headerCb && rows && rows.length > 0) {
      var selectedVisible = rows.filter(function(r) { return selectedExecutors[r.id]; }).length;
      headerCb.checked = selectedVisible === rows.length;
      headerCb.indeterminate = selectedVisible > 0 && selectedVisible < rows.length;
    }
  }
  function renderLegend() {
    var legendRows = applyMonitorSelection(latestVisibleRows);
    var items = legendRows
      .map(function(row) { return executors[row.id] ? { id: row.id, series: executors[row.id] } : null; })
      .filter(function(item) { return item != null; });
    if (items.length === 0) {
      legendNode.innerHTML = '';
      return;
    }
    var selected = getHeapMetricSelection();
    legendNode.innerHTML = items.map(function(item) {
      var parts = [];
      if (selected.heap) parts.push('H ' + fmtMB(latestValue(item.series.heapData)));
      if (selected.overhead) parts.push('O ' + fmtMB(latestValue(item.series.overheadData)));
      if (selected.offHeap) parts.push('F ' + fmtMB(latestValue(item.series.offHeapData)));
      // "Native/Other" is derived from RSS regardless of whether the "Total" line itself is
      // shown, so it's driven solely by its own checkbox (selected.nativeOther) — consistent with
      // how Heap/Overhead/Spark Off-Heap are each controlled independently of one another.
      var useRss = totalMemoryMode === 'rss';
      var totalSeries = useRss ? (item.series.rssData || []) : item.series.totalMemoryData;
      var totalLatest = latestValue(totalSeries);
      if (selected.nativeOther && useRss && totalLatest != null) {
        // Mirrors the "Native/Other" stacked segment in buildMemoryChartSeries: the part of the
        // real RSS not accounted for by the ACTUAL H+O+F sum (thread stacks, native/direct
        // buffers, native allocator overhead, ...) — NOT metaspace/code cache, which is already
        // inside "Overhead" (JVMOffHeapMemory). Computed against the real sum regardless of which
        // of H/O/F are currently toggled for display, so the value never changes just because
        // Heap/Overhead/Spark Off-Heap are shown/hidden.
        var fullSum = (latestValue(item.series.heapData) || 0) +
          (latestValue(item.series.overheadData) || 0) +
          (latestValue(item.series.offHeapData) || 0);
        var gapLatest = totalLatest - fullSum;
        if (gapLatest > 0) parts.push('N ' + fmtMB(gapLatest));
      }
      if (selected.total) {
        // No implicit fallback: in 'rss' mode always show the real RSS series (which may be all
        // null/gaps if spark.executor.processTreeMetrics.enabled isn't set — see resolveTotalMemory).
        parts.push((useRss ? 'T(rss) ' : 'T ') + fmtMB(totalLatest));
      }
      return '<span class="heap-legend-item">' +
        '<span class="heap-legend-color" style="background:' + item.series.color + ';"></span>' +
        '<span>' + escapeHtml(item.series.label) + '</span>' +
        '<strong>' + escapeHtml(parts.join(' | ')) + '</strong>' +
        '</span>';
    }).join('');
  }
  function buildMemoryChartSeries(allowedIds) {
    var allowed = {};
    (allowedIds || []).forEach(function(id) { allowed[id] = true; });
    var selected = getHeapMetricSelection();
    var items = [];
    Object.keys(executors)
      .sort(function(a, b) {
        if (a === 'driver') return -1;
        if (b === 'driver') return 1;
        return a.localeCompare(b, undefined, { numeric: true, sensitivity: 'base' });
      })
      .forEach(function(id) {
        if (allowedIds != null && !allowed[id]) return;
        var series = executors[id];
        var baseColor = series.color || '#888888';
        var components = [
          { enabled: selected.heap, key: 'heapData', suffix: 'H', color: shadeColor(baseColor, 0.00), fillAlpha: 0.26 },
          { enabled: selected.overhead, key: 'overheadData', suffix: 'O', color: shadeColor(baseColor, 0.30), fillAlpha: 0.24 },
          { enabled: selected.offHeap, key: 'offHeapData', suffix: 'F', color: shadeColor(baseColor, 0.55), fillAlpha: 0.22 }
        ];
        var ranked = components
          .map(function(c) {
            var data = Array.isArray(series[c.key]) ? series[c.key] : [];
            return { meta: c, data: data, latest: latestValue(data) };
          })
          .filter(function(x) { return x.meta.enabled && x.data.some(function(v) { return v != null; }); })
          .sort(function(a, b) {
            var av = a.latest == null ? -Infinity : a.latest;
            var bv = b.latest == null ? -Infinity : b.latest;
            return bv - av;
          });

        var n = labels.length;
        var cumulative = new Array(n);
        for (var ci = 0; ci < n; ci += 1) cumulative[ci] = 0;

        ranked.forEach(function(entry) {
          var base = cumulative.slice();
          var upper = new Array(n);
          for (var i = 0; i < n; i += 1) {
            var v = entry.data[i];
            if (v == null) {
              upper[i] = null;
            } else {
              upper[i] = base[i] + v;
              cumulative[i] = upper[i];
            }
          }
          items.push({
            id: id,
            series: { color: entry.meta.color },
            data: upper,
            stackBaseData: base,
            lineDash: [],
            lineWidth: 1.8,
            labelSuffix: entry.meta.suffix,
            fillAlpha: entry.meta.fillAlpha
          });
        });

        // "Native/Other" is derived from RSS regardless of whether the "Total" line itself is
        // drawn, so it's gated solely by its own checkbox (selected.nativeOther) — consistent with
        // how Heap/Overhead/Spark Off-Heap are each independent of one another and of "Total".
        // No implicit fallback: in 'rss' mode always use the real RSS series, even if that means
        // a mostly/fully empty series — that's the visible signal that
        // spark.executor.processTreeMetrics.enabled isn't set (see resolveTotalMemory / the
        // rssMissingWarning banner), rather than silently substituting the estimate.
        var useRss = totalMemoryMode === 'rss';
        var totalData = useRss ? (Array.isArray(series.rssData) ? series.rssData : []) : (Array.isArray(series.totalMemoryData) ? series.totalMemoryData : []);
        var hasTotalData = totalData.some(function(v) { return v != null; });

        if (useRss && selected.nativeOther && hasTotalData) {
          // In RSS mode, the real OS-level RSS is virtually always higher than the *actual* H+O+F
          // sum: it also accounts for thread stacks and native/direct buffers (Netty, Snappy/Zstd,
          // Arrow, ...) that Spark's own counters don't track. (Metaspace/Code Cache is already
          // inside "Overhead" = JVMOffHeapMemory, so it's NOT part of this gap.)
          // The gap amount must be computed against the REAL H+O+F sum (regardless of which of
          // those are currently toggled on/off for display) so that toggling Heap/Overhead/
          // Spark Off-Heap on or off never changes how much "Native/Other" represents — it's then
          // stacked on top of whatever *is* currently displayed (cumulative), so with everything
          // else hidden it shows just its own thin band rather than filling all the way to Total.
          var heapLatestArr = Array.isArray(series.heapData) ? series.heapData : [];
          var overheadLatestArr = Array.isArray(series.overheadData) ? series.overheadData : [];
          var offHeapLatestArr = Array.isArray(series.offHeapData) ? series.offHeapData : [];
          var gapBase = cumulative.slice();
          var gapUpper = new Array(n);
          var hasGap = false;
          for (var gi = 0; gi < n; gi += 1) {
            var tv = totalData[gi];
            var cv = cumulative[gi];
            var hv = heapLatestArr[gi];
            var ov = overheadLatestArr[gi];
            var fv = offHeapLatestArr[gi];
            if (tv == null || cv == null) {
              gapUpper[gi] = null;
            } else {
              var fullSum = (hv == null ? 0 : hv) + (ov == null ? 0 : ov) + (fv == null ? 0 : fv);
              var gapAmount = tv - fullSum;
              if (gapAmount > 0) {
                gapUpper[gi] = cv + gapAmount;
                hasGap = true;
              } else {
                // RSS <= real H+O+F sum for this sample (measurement timing skew, or a transient
                // sample right after JVM startup): no native/other segment to draw for this point.
                gapUpper[gi] = cv;
              }
            }
          }
          if (hasGap) {
            items.push({
              id: id,
              series: { color: shadeColor(baseColor, 0.78) },
              data: gapUpper,
              stackBaseData: gapBase,
              lineDash: [2, 2],
              lineWidth: 1.2,
              labelSuffix: 'N',
              fillAlpha: 0.16
            });
            cumulative = gapUpper;
          }
        }

        if (selected.total && hasTotalData) {
          items.push({
            id: id,
            series: { color: shadeColor(baseColor, -0.22) },
            data: totalData,
            lineDash: [8, 3],
            lineWidth: 2.6,
            labelSuffix: useRss ? 'T(rss)' : 'T',
            fillAlpha: 0.0
          });
        }
       });
     return items;
   }
   function numericDesc(a, b) {
    var av = a == null ? -Infinity : a;
    var bv = b == null ? -Infinity : b;
    return bv - av;
  }
  function topRows(rows, accessor, limit) {
    return rows
      .filter(function(row) { return accessor(row) != null; })
      .slice()
      .sort(function(a, b) { return numericDesc(accessor(a), accessor(b)); })
      .slice(0, limit);
  }
  function renderTopOffenders(rows) {
    if (rows.length === 0) {
      topOffendersNode.innerHTML = '<div class="heap-topoffender-card"><div class="heap-empty-state">No executors to show with current filters.</div></div>';
      return;
    }
    var sections = [
      { title: 'Top Heap % (Xmx)', rows: topRows(rows, function(row) { return row.heapPct; }, 3), format: function(row) { return fmtPct(row.heapPct); } },
      { title: 'Top Total Memory', rows: topRows(rows, function(row) { return row.totalMemoryMB; }, 3), format: function(row) { return fmtMB(row.totalMemoryMB) + ' MB'; } },
      { title: 'Top GC ∆ last poll', rows: topRows(rows, function(row) { return row.gcDeltaMs != null ? row.gcDeltaMs : null; }, 3), format: function(row) { var ms = row.gcDeltaMs; return ms < 1000 ? ms.toFixed(0) + ' ms' : (ms/1000).toFixed(2) + ' s'; } },
      { title: 'Top Saturation %', rows: topRows(rows, function(row) { return row.saturationPct; }, 3), format: function(row) { return fmtPct(row.saturationPct); } },
      { title: 'Top JVM Off-Heap (MB)', rows: topRows(rows, function(row) { return row.jvmOffHeapMB; }, 3), format: function(row) { return fmtMB(row.jvmOffHeapMB) + ' MB'; } },
      { title: 'Top On-Heap Unified (MB)', rows: topRows(rows, function(row) { return row.onHeapUnifiedMB; }, 3), format: function(row) { return fmtMB(row.onHeapUnifiedMB) + ' MB'; } },
      { title: 'Top Shuffle Read', rows: topRows(rows, function(row) { return row.shuffleReadMB; }, 3), format: function(row) { return fmtMB(row.shuffleReadMB) + ' MB'; } }
    ];
    topOffendersNode.innerHTML = sections.map(function(section) {
      var items = section.rows.length > 0
        ? '<ol class="heap-topoffender-list">' + section.rows.map(function(row) {
            return '<li><strong>' + escapeHtml(row.id) + '</strong> @ ' + escapeHtml(row.host) + ' — ' + escapeHtml(section.format(row)) + '</li>';
          }).join('') + '</ol>'
        : '<div class="heap-empty-state">No data available.</div>';
      return '<div class="heap-topoffender-card">' +
        '<div class="heap-topoffender-title">' + escapeHtml(section.title) + '</div>' +
        items +
        '</div>';
    }).join('');
  }
  function renderSummary(rows) {
    var heapPctValues = [];
    var gcPctValues = [];
    var failedTasks = 0;
    var activeTasks = 0;
    var shuffleReadMb = 0;
    var shuffleWriteMb = 0;
    var unhealthyCount = 0;
    var diskUsedMb = 0;
    var memoryUsedMb = 0;
    var jvmOffHeapMbTotal = 0;
    var sparkOffHeapMbTotal = 0;
    var totalMemoryMbTotal = 0;
    var onHeapUnifiedMbMax = null;
    rows.forEach(function(ex) {
      if (ex.heapPct != null) heapPctValues.push(ex.heapPct);
      var gcVal = ex.gcDeltaMs != null ? ex.gcDeltaMs : null;
      if (gcVal != null) gcPctValues.push(gcVal);
      failedTasks += ex.failedTasks || 0;
      activeTasks += ex.activeTasks || 0;
      shuffleReadMb += ex.shuffleReadMB || 0;
      shuffleWriteMb += ex.shuffleWriteMB || 0;
      diskUsedMb += ex.diskUsedMB || 0;
      memoryUsedMb += ex.memoryUsedMB || 0;
      jvmOffHeapMbTotal += ex.jvmOffHeapMB || 0;
      sparkOffHeapMbTotal += ex.offHeapUnifiedMB || 0;
      totalMemoryMbTotal += ex.totalMemoryMB || 0;
      if (ex.onHeapUnifiedMB != null && (onHeapUnifiedMbMax == null || ex.onHeapUnifiedMB > onHeapUnifiedMbMax)) onHeapUnifiedMbMax = ex.onHeapUnifiedMB;
      if (ex.isUnhealthy) unhealthyCount += 1;
    });
    var avgHeapPct = heapPctValues.length > 0
      ? heapPctValues.reduce(function(a, b) { return a + b; }, 0) / heapPctValues.length
      : null;
    var maxHeapPct = heapPctValues.length > 0 ? Math.max.apply(null, heapPctValues) : null;
    var avgGcPct = gcPctValues.length > 0
      ? gcPctValues.reduce(function(a, b) { return a + b; }, 0) / gcPctValues.length
      : null;
    var cards = [
      { title: 'Executors', value: rows.length, note: 'monitored', kind: 'normal' },
      { title: 'Unhealthy', value: fmtInt(unhealthyCount), note: 'with current thresholds', kind: unhealthyCount > 0 ? 'warn' : 'normal' },
      { title: 'Avg Heap %', value: fmtPct(avgHeapPct), note: 'executor average', kind: avgHeapPct != null && avgHeapPct >= 85 ? 'warn' : 'normal' },
      { title: 'Max Heap %', value: fmtPct(maxHeapPct), note: 'current peak', kind: maxHeapPct != null && maxHeapPct >= 90 ? 'danger' : (maxHeapPct != null && maxHeapPct >= 85 ? 'warn' : 'normal') },
      { title: 'Active Tasks', value: fmtInt(activeTasks), note: 'current sum', kind: 'normal' },
      { title: 'Failed Tasks', value: fmtInt(failedTasks), note: 'cumulative', kind: failedTasks > 0 ? 'danger' : 'normal' },
      { title: 'Avg GC ∆', value: avgGcPct != null ? (avgGcPct < 1000 ? avgGcPct.toFixed(0) + ' ms' : (avgGcPct/1000).toFixed(2) + ' s') : '-', note: 'GC time per poll', kind: avgGcPct != null && avgGcPct >= 2000 ? 'danger' : (avgGcPct != null && avgGcPct >= 500 ? 'warn' : 'normal') },
      { title: 'Shuffle Read', value: fmtMB(shuffleReadMb) + ' MB', note: 'current total', kind: 'normal' },
      { title: 'Shuffle Write', value: fmtMB(shuffleWriteMb) + ' MB', note: 'current total', kind: 'normal' },
      { title: 'Disk Used', value: fmtMB(diskUsedMb) + ' MB', note: 'total storage', kind: 'normal' },
      { title: 'Storage Memory', value: fmtMB(memoryUsedMb) + ' MB', note: 'total memoryUsed', kind: 'normal' },
      { title: 'JVM Off-Heap', value: fmtMB(jvmOffHeapMbTotal) + ' MB', note: 'executor sum (overhead)', kind: 'normal' },
      { title: 'Spark Off-Heap', value: fmtMB(sparkOffHeapMbTotal) + ' MB', note: 'total unified off-heap', kind: 'normal' },
      { title: 'Total Memory', value: fmtMB(totalMemoryMbTotal) + ' MB', note: 'heap + overhead + off-heap', kind: 'normal' },
      { title: 'Max On-Heap Unified', value: fmtMB(onHeapUnifiedMbMax) + ' MB', note: 'exec+storage peak', kind: 'normal' }
    ];
    summaryNode.innerHTML = cards.map(function(card) {
      return '<div class="heap-summary-card">' +
        '<div class="heap-summary-title">' + escapeHtml(card.title) + '</div>' +
        '<div class="heap-summary-value" style="' + cardStyle(card.kind) + '">' + escapeHtml(String(card.value)) + '</div>' +
        '<div class="heap-summary-note">' + escapeHtml(card.note) + '</div>' +
        '</div>';
    }).join('');
  }
""" +
    """
  function renderMetricChart(canvasNode, containerNode, context, metricKey, title, unitLabel, emptyMessage, allowedIds, ceilingOverride, customSeriesItems) {
    var chartHeight;
    if (metricKey === 'heapData') chartHeight = layoutState.heights.memory;
    else if (metricKey === 'cpuData') chartHeight = layoutState.heights.cpu;
    else if (metricKey === 'gcData') chartHeight = layoutState.heights.gc;
    else if (metricKey === 'saturationData') chartHeight = layoutState.heights.saturation;
    else chartHeight = 240;
    var size = syncCanvasSize(canvasNode, containerNode, chartHeight);
    var width = size.width;
    var height = size.height;
    var dpr = window.devicePixelRatio || 1;
    context.setTransform(dpr, 0, 0, dpr, 0, 0);
    context.clearRect(0, 0, width, height);

    var seriesItems = customSeriesItems || visibleSeries(metricKey, allowedIds);
    if (seriesItems.length === 0 || labels.length === 0) {
      context.fillStyle = '#666';
      context.font = '14px sans-serif';
      context.fillText(emptyMessage, 16, 24);
      canvasNode._chartTip = null;
      bindChartTooltip(canvasNode);
      return;
    }

    var padding = { top: 20, right: 90, bottom: 44, left: 58 };
    var plotLeft = padding.left;
    var plotTop = padding.top;
    var plotWidth = Math.max(width - padding.left - padding.right, 80);
    var plotHeight = Math.max(height - padding.top - padding.bottom, 80);
    var values = [];

    seriesItems.forEach(function(item) {
      item.data.forEach(function(v) {
        if (v != null) values.push(v);
      });
    });
    var maxY = values.length > 0 ? niceCeiling(Math.max.apply(null, values) * 1.1) : 1;
    if (ceilingOverride != null) maxY = Math.max(maxY, ceilingOverride);
    if (!Number.isFinite(maxY) || maxY <= 0) maxY = 1;

    function xAt(index) {
      if (labels.length <= 1) return plotLeft;
      return plotLeft + (plotWidth * index / (labels.length - 1));
    }
    function yAt(value) {
      return plotTop + plotHeight - ((value / maxY) * plotHeight);
    }

    context.strokeStyle = '#ddd';
    context.lineWidth = 1;
    context.beginPath();
    context.rect(plotLeft, plotTop, plotWidth, plotHeight);
    context.stroke();

    var ticks = 5;
    context.font = '12px sans-serif';
    context.fillStyle = '#666';
    for (var t = 0; t <= ticks; t += 1) {
      var ratio = t / ticks;
      var y = plotTop + plotHeight - (ratio * plotHeight);
      var tickValue = maxY * ratio;
      context.strokeStyle = '#eee';
      context.beginPath();
      context.moveTo(plotLeft, y);
      context.lineTo(plotLeft + plotWidth, y);
      context.stroke();
      context.fillText(tickValue.toFixed(0) + ' ' + unitLabel, 8, y + 4);
    }

    var xTickCount = Math.min(labels.length, 6);
    for (var xi = 0; xi < xTickCount; xi += 1) {
      var index = xTickCount === 1 ? labels.length - 1 : Math.round((xi * (labels.length - 1)) / (xTickCount - 1));
      var x = xAt(index);
      context.strokeStyle = '#f3f3f3';
      context.beginPath();
      context.moveTo(x, plotTop);
      context.lineTo(x, plotTop + plotHeight);
      context.stroke();
      context.fillStyle = '#666';
      context.fillText(labels[index], Math.max(x - 18, plotLeft), plotTop + plotHeight + 18);
    }

    seriesItems.forEach(function(item) {
      var series = item.data;

      if (item.fillAlpha != null && item.fillAlpha > 0) {
        context.fillStyle = colorWithAlpha(item.series.color, item.fillAlpha);
        var baseSeries = Array.isArray(item.stackBaseData) ? item.stackBaseData : null;
        var segStart = -1;
        for (var fi = 0; fi <= series.length; fi += 1) {
          var inSeg = fi < series.length && series[fi] != null && (!baseSeries || baseSeries[fi] != null);
          if (inSeg && segStart < 0) segStart = fi;
          if ((!inSeg || fi === series.length) && segStart >= 0) {
            var segEnd = fi - 1;
            context.beginPath();
            context.moveTo(xAt(segStart), yAt(series[segStart]));
            for (var sf = segStart + 1; sf <= segEnd; sf += 1) {
              context.lineTo(xAt(sf), yAt(series[sf]));
            }
            for (var sb = segEnd; sb >= segStart; sb -= 1) {
              var baseY = baseSeries ? yAt(baseSeries[sb]) : (plotTop + plotHeight);
              context.lineTo(xAt(sb), baseY);
            }
            context.closePath();
            context.fill();
            segStart = -1;
          }
        }
      }

      var started = false;
      context.strokeStyle = item.series.color;
      context.lineWidth = item.lineWidth || 2;
      context.setLineDash(item.lineDash || []);
      context.beginPath();
      for (var i = 0; i < series.length; i += 1) {
        var point = series[i];
        if (point == null) {
          started = false;
        } else {
          var px = xAt(i);
          var py = yAt(point);
          if (!started) {
            context.moveTo(px, py);
            started = true;
          } else {
            context.lineTo(px, py);
          }
        }
      }
      context.stroke();
      context.setLineDash([]);

      for (var j = 0; j < series.length; j += 1) {
        var v = series[j];
        if (v != null) {
          context.fillStyle = item.series.color;
          context.beginPath();
          context.arc(xAt(j), yAt(v), 2.5, 0, Math.PI * 2);
          context.fill();
        }
      }
      // Label the executor ID at the end of the series
      var lastIdxL = -1, lastValL = null;
      for (var li = series.length - 1; li >= 0; li -= 1) {
        if (series[li] != null) { lastIdxL = li; lastValL = series[li]; break; }
      }
      if (lastIdxL >= 0) {
        var shortId = item.id.length > 8 ? item.id.slice(-8) : item.id;
        if (item.labelSuffix) shortId += '-' + item.labelSuffix;
        context.fillStyle = item.series.color;
        context.font = 'bold 10px sans-serif';
        context.fillText(shortId, plotLeft + plotWidth + 6, yAt(lastValL) + 4);
      }
    });

    context.fillStyle = '#333';
    context.font = 'bold 13px sans-serif';
    context.fillText(title, plotLeft, 14);
    context.font = '12px sans-serif';
    context.fillText('Time', plotLeft + plotWidth - 24, plotTop + plotHeight + 34);

    // Store this render's geometry/series on the canvas so the shared mousemove/mouseleave
    // handlers (bound once per canvas by bindChartTooltip) can hit-test the cursor position and
    // show an exact-value tooltip without re-computing the whole chart layout.
    canvasNode._chartTip = {
      labels: labels.slice(),
      seriesItems: seriesItems,
      plotLeft: plotLeft,
      plotTop: plotTop,
      plotWidth: plotWidth,
      plotHeight: plotHeight,
      unitLabel: unitLabel,
      title: title
    };
    bindChartTooltip(canvasNode);
  }
  /**
   * Binds the hover tooltip mousemove/mouseleave handlers to a chart canvas exactly once (the
   * `_tooltipBound` flag survives across re-renders, which happen every poll). The handlers always
   * read the *current* `canvasNode._chartTip` (refreshed by every renderMetricChart call), so they
   * stay correct across re-renders without needing to be re-attached.
   */
  function bindChartTooltip(canvasNode) {
    if (canvasNode._tooltipBound) return;
    canvasNode._tooltipBound = true;
    canvasNode.addEventListener('mousemove', function(evt) { showChartTooltip(canvasNode, evt); });
    canvasNode.addEventListener('mouseleave', hideChartTooltip);
  }
  /**
   * Finds the data point nearest the cursor's x position and shows a small fixed-position tooltip
   * with the exact value (not the cumulative stack height) of every series passing through that
   * point — including stacked memory-chart segments (Heap/Overhead/Off-Heap/Native-Other), whose
   * `item.data` holds the *cumulative* top of the stack rather than each layer's own value; the
   * `stackBaseData` subtraction below recovers the individual metric reading in both cases (it's a
   * no-op — subtracting 0 — for plain, non-stacked lines like Total/GC/CPU/Saturation).
   */
  function showChartTooltip(canvasNode, evt) {
    var g = canvasNode._chartTip;
    var tip = document.getElementById('chartTooltip');
    if (!g || !tip || !g.seriesItems || g.seriesItems.length === 0 || g.labels.length === 0) {
      hideChartTooltip();
      return;
    }
    var rect = canvasNode.getBoundingClientRect();
    var mouseX = evt.clientX - rect.left;
    var mouseY = evt.clientY - rect.top;
    if (mouseX < g.plotLeft || mouseX > g.plotLeft + g.plotWidth ||
        mouseY < g.plotTop || mouseY > g.plotTop + g.plotHeight) {
      hideChartTooltip();
      return;
    }
    var n = g.labels.length;
    var idx = n <= 1 ? 0 : Math.round((mouseX - g.plotLeft) * (n - 1) / g.plotWidth);
    idx = Math.max(0, Math.min(n - 1, idx));

    var rows = [];
    g.seriesItems.forEach(function(item) {
      var upper = item.data[idx];
      if (upper == null) return;
      var base = (Array.isArray(item.stackBaseData) && item.stackBaseData[idx] != null) ? item.stackBaseData[idx] : 0;
      var value = upper - base;
      var shortId = item.id.length > 8 ? item.id.slice(-8) : item.id;
      var label = shortId + (item.labelSuffix ? ' (' + item.labelSuffix + ')' : '');
      rows.push({ color: item.series.color, label: label, value: value });
    });
    if (rows.length === 0) {
      hideChartTooltip();
      return;
    }

    var html = '<div style="font-weight:bold;margin-bottom:4px;">' + escapeHtml(g.labels[idx]) + '</div>' +
      rows.map(function(r) {
        return '<div class="chart-tooltip-row">' +
          '<span class="chart-tooltip-dot" style="background:' + r.color + ';"></span>' +
          escapeHtml(r.label) + ':&nbsp;<strong>' + r.value.toFixed(2) + ' ' + escapeHtml(g.unitLabel) + '</strong>' +
          '</div>';
      }).join('');
    tip.innerHTML = html;
    tip.style.display = 'block';

    // Keep the tooltip within the viewport: flip to the left/above the cursor near the edges
    // instead of letting it overflow off-screen.
    var tipWidth = tip.offsetWidth || 200;
    var tipHeight = tip.offsetHeight || 60;
    var left = evt.clientX + 14;
    var top = evt.clientY + 14;
    if (left + tipWidth > window.innerWidth) left = evt.clientX - tipWidth - 14;
    if (top + tipHeight > window.innerHeight) top = evt.clientY - tipHeight - 14;
    tip.style.left = Math.max(4, left) + 'px';
    tip.style.top = Math.max(4, top) + 'px';
  }
  function hideChartTooltip() {
    var tip = document.getElementById('chartTooltip');
    if (tip) tip.style.display = 'none';
  }
  function isRowUnhealthy(row, warnPct, satWarnPct) {
    return (row.heapPct != null && row.heapPct >= warnPct) ||
      (row.gcDeltaMs != null && row.gcDeltaMs >= 500) ||
      (row.gcPct != null && row.gcPct >= 10) ||
      (row.saturationPct != null && row.saturationPct >= satWarnPct) ||
      (row.failedTasks != null && row.failedTasks > 0);
  }
  function toRow(ex, warnPct, satWarnPct) {
    var id = String(ex.id);
    var currentReading = heapCurrentOf(ex);
    var heapB = currentReading.bytes;
    var maxB = heapMaxOf(ex);
    var gcSeconds = gcSecondsOf(ex);
    var gcPct = gcPctOf(ex);
    var gcDeltaMs = executors[id] ? (executors[id].lastGcDeltaMs != null ? executors[id].lastGcDeltaMs : null) : null;
    var activeTasks = toNum(ex.activeTasks);
    var maxTasks = taskCapacityOf(ex);
    var saturationPct = saturationPctOf(ex);
    var cpuPct = executors[id] ? (executors[id].lastCpuPct != null ? executors[id].lastCpuPct : null) : null;
    var cpuSource = executors[id] ? (executors[id].lastCpuSource || '-') : '-';
    var failedTasks = toNum(ex.failedTasks);
    var shuffleReadMB = toMB(toNum(ex.totalShuffleRead) || 0);
    var shuffleWriteMB = toMB(toNum(ex.totalShuffleWrite) || 0);
    var onHeapStorageB = ex.memoryMetrics ? toNum(ex.memoryMetrics.usedOnHeapStorageMemory) : null;
    var totalOnHeapCapacityB = ex.memoryMetrics ? toNum(ex.memoryMetrics.totalOnHeapStorageMemory) : null;
    var rddBlocks = toNum(ex.rddBlocks);
    var diskUsedB = toNum(ex.diskUsed);
    var memoryUsedB = toNum(ex.memoryUsed);
    // peakMemoryMetrics fields — aligned with spark-monitor
    var jvmOffHeapB          = ex.peakMemoryMetrics ? toNum(ex.peakMemoryMetrics.JVMOffHeapMemory)         : null;
    var offHeapUnifiedB      = ex.peakMemoryMetrics ? toNum(ex.peakMemoryMetrics.OffHeapUnifiedMemory)     : null;
    var onHeapExecB          = ex.peakMemoryMetrics ? toNum(ex.peakMemoryMetrics.OnHeapExecutionMemory)    : null;
    var onHeapStoragePeakB   = ex.peakMemoryMetrics ? toNum(ex.peakMemoryMetrics.OnHeapStorageMemory)      : null;
    var onHeapUnifiedB       = ex.peakMemoryMetrics ? toNum(ex.peakMemoryMetrics.OnHeapUnifiedMemory)      : null;
    var offHeapUnifiedMB     = offHeapUnifiedB != null ? toMB(offHeapUnifiedB) : null;
    var rssB                 = ex.peakMemoryMetrics ? toNum(ex.peakMemoryMetrics.ProcessTreeJVMRSSMemory) : null;
    var rssMB                 = rssB != null ? toMB(rssB) : null;
    var resolvedTotal        = resolveTotalMemory(heapB != null ? toMB(heapB) : null, jvmOffHeapB != null ? toMB(jvmOffHeapB) : null, offHeapUnifiedMB, rssMB);
    var totalMemoryMB        = resolvedTotal.value;
    var row = {
      id: id,
      host: ex.hostPort || ex.host || '-',
      heapB: heapB,
      heapMB: heapB != null ? toMB(heapB) : null,
      heapMaxB: maxB,
      heapMaxMB: toMB(maxB),
      heapPct: (heapB != null && maxB > 0) ? ((heapB * 100.0) / maxB) : null,
      jvmOffHeapMB:        jvmOffHeapB        != null ? toMB(jvmOffHeapB)        : null,
      offHeapUnifiedMB: offHeapUnifiedMB,
      rssMB: rssMB,
      totalMemoryMB: totalMemoryMB,
      totalMemorySource: resolvedTotal.source,
      nativeOtherMB: nativeOtherMBOf(heapB != null ? toMB(heapB) : null, jvmOffHeapB != null ? toMB(jvmOffHeapB) : null, offHeapUnifiedMB, totalMemoryMB, resolvedTotal.source),
      gcSeconds: gcSeconds,
      gcPct: gcPct,
      gcDeltaMs: gcDeltaMs,
      activeTasks: activeTasks,
      maxTasks: maxTasks,
      saturationPct: saturationPct,
      cpuPct: cpuPct,
      cpuSource: cpuSource,
      failedTasks: failedTasks,
      shuffleReadMB: shuffleReadMB,
      shuffleWriteMB: shuffleWriteMB,
      rddBlocks: rddBlocks,
      diskUsedB: diskUsedB,
      diskUsedMB: diskUsedB != null ? toMB(diskUsedB) : null,
      memoryUsedB: memoryUsedB,
      memoryUsedMB: memoryUsedB != null ? toMB(memoryUsedB) : null,
      onHeapStorageB: onHeapStorageB,
      onHeapStorageMB: onHeapStorageB != null ? toMB(onHeapStorageB) : null,
      totalOnHeapCapacityMB: totalOnHeapCapacityB != null ? toMB(totalOnHeapCapacityB) : null,
      onHeapExecMB:        onHeapExecB        != null ? toMB(onHeapExecB)        : null,
      onHeapStoragePeakMB: onHeapStoragePeakB != null ? toMB(onHeapStoragePeakB) : null,
      onHeapUnifiedMB:     onHeapUnifiedB     != null ? toMB(onHeapUnifiedB)     : null,
      metricSource: currentReading.source
    };
    row.isUnhealthy = isRowUnhealthy(row, warnPct, satWarnPct);
    return row;
  }
  function compareValues(a, b, direction) {
    var multiplier = direction === 'asc' ? 1 : -1;
    var aNull = (a == null || a === '');
    var bNull = (b == null || b === '');
    if (aNull && bNull) return 0;
    if (aNull) return 1;
    if (bNull) return -1;
    if (typeof a === 'number' && typeof b === 'number') return (a - b) * multiplier;
    return String(a).localeCompare(String(b), undefined, { numeric: true, sensitivity: 'base' }) * multiplier;
  }
  function sortRows(rows) {
    var key = sortState.key;
    var direction = sortState.direction;
    return rows.slice().sort(function(a, b) {
      var primary = compareValues(a[key], b[key], direction);
      if (primary !== 0) return primary;
      return compareValues(a.id, b.id, 'asc');
    });
  }
  function updateSortHeaders() {
    tableHeaders.forEach(function(header) {
      var label = header.getAttribute('data-label') || header.textContent;
      var key = header.getAttribute('data-sort-key');
      var marker = key === sortState.key ? (sortState.direction === 'asc' ? ' ↑' : ' ↓') : '';
      header.textContent = label + marker;
    });
  }
  function renderCharts(rows) {
    var allowedIds = rows.map(function(row) { return row.id; });
    var memorySeriesItems = buildMemoryChartSeries(allowedIds);
    renderMetricChart(
      canvas,
      chartContainer,
      ctx,
      'heapData',
      'Memory',
      'MB',
      'Select at least one memory metric or wait for available data.',
      allowedIds,
      null,
      memorySeriesItems.length > 0 ? memorySeriesItems : null
    );
    renderMetricChart(gcCanvas, gcChartContainer, gcCtx, 'gcData', 'GC ∆ (ms)', 'ms', 'No GC data available (wait for the 2nd poll).', allowedIds, null);
    // No fixed y-axis floor: scale dynamically to the actual data range (with headroom), same as
    // the Memory chart, instead of forcing the axis up to a fixed threshold (100%/satWarnPct) that
    // would squash the line whenever real values are much lower than that threshold.
    renderMetricChart(saturationCanvas, saturationChartContainer, saturationCtx, 'saturationData', 'Task Saturation %', '%', 'No task saturation data available.', allowedIds, null);
    renderMetricChart(cpuCanvas, cpuChartContainer, cpuCtx, 'cpuData', 'CPU % (utilization)', '%', 'No CPU data available (wait for the 2nd poll).', allowedIds, null);
  }
  function renderTable(rows, warnPct, satWarnPct) {
    var tbody = document.getElementById('heapTableBody');
    tbody.innerHTML = '';
    if (rows.length === 0) {
      tbody.innerHTML = '<tr><td colspan="25" class="heap-empty-state">No executors to display with current filters.</td></tr>';
      prevTableValuesByExecutor = {};
      return;
    }
    var sortedRows = sortRows(rows);
    var nextTableValuesByExecutor = {};

    function changeClass(prev, curr) {
      if (prev == null || curr == null) return '';
      if (prev === curr) return '';
      return curr > prev ? 'heap-cell-changed-up' : 'heap-cell-changed-down';
    }

    function tdWithChange(html, styleAttr, className) {
      var cls = className ? (' class="' + className + '"') : '';
      return '<td' + cls + (styleAttr || '') + '>' + html + '</td>';
    }

    sortedRows.forEach(function(row) {
      var heapPctStyle = row.heapPct != null && row.heapPct >= warnPct ? ' style="color:#a94442;font-weight:bold;"' : '';
      var gcDeltaStyle = row.gcDeltaMs != null && row.gcDeltaMs >= 2000 ? ' style="color:#a94442;font-weight:bold;"'
        : (row.gcDeltaMs != null && row.gcDeltaMs >= 500 ? ' style="color:#8a6d3b;font-weight:bold;"' : '');
      var saturationStyle = row.saturationPct != null && row.saturationPct >= satWarnPct ? ' style="color:#a94442;font-weight:bold;"'
        : (row.saturationPct != null && row.saturationPct >= satWarnPct * 0.75 ? ' style="color:#8a6d3b;font-weight:bold;"' : '');
      var cpuStyle = row.cpuPct != null && row.cpuPct >= 50 ? ' style="color:#a94442;font-weight:bold;"'
        : (row.cpuPct != null && row.cpuPct >= 20 ? ' style="color:#8a6d3b;font-weight:bold;"' : '');
      var failedTasksStyle = row.failedTasks != null && row.failedTasks > 0 ? ' style="color:#a94442;font-weight:bold;"' : '';
      var c = executors[row.id] ? executors[row.id].color : '#888';

      var currVals = {
        heapMB: row.heapMB,
        heapMaxMB: row.heapMaxMB,
        heapPct: row.heapPct,
        jvmOffHeapMB: row.jvmOffHeapMB,
        jvmOffHeapPct: pctOfTotal(row.jvmOffHeapMB, row.totalMemoryMB),
        offHeapUnifiedMB: row.offHeapUnifiedMB,
        offHeapUnifiedPct: pctOfTotal(row.offHeapUnifiedMB, row.totalMemoryMB),
        totalMemoryMB: row.totalMemoryMB,
        nativeOtherMB: row.nativeOtherMB,
        nativeOtherPct: pctOfTotal(row.nativeOtherMB, row.totalMemoryMB),
        gcSeconds: row.gcSeconds,
        gcDeltaMs: row.gcDeltaMs,
        activeTasks: row.activeTasks,
        maxTasks: row.maxTasks,
        saturationPct: row.saturationPct,
        cpuPct: row.cpuPct,
        failedTasks: row.failedTasks,
        shuffleReadMB: row.shuffleReadMB,
        shuffleWriteMB: row.shuffleWriteMB,
        rddBlocks: row.rddBlocks,
        diskUsedMB: row.diskUsedMB,
        memoryUsedMB: row.memoryUsedMB,
        onHeapStorageMB: row.onHeapStorageMB,
        totalOnHeapCapacityMB: row.totalOnHeapCapacityMB,
        onHeapExecMB: row.onHeapExecMB,
        onHeapStoragePeakMB: row.onHeapStoragePeakMB,
        onHeapUnifiedMB: row.onHeapUnifiedMB
      };
      nextTableValuesByExecutor[row.id] = currVals;
      var prevVals = prevTableValuesByExecutor[row.id] || {};

      tbody.innerHTML +=
        '<tr' + (row.isUnhealthy ? ' class="heap-table-row-unhealthy"' : '') + '>' +
        '<td><input type="checkbox" class="monitor-exec-checkbox" data-exec-id="' + escapeHtml(row.id) + '" ' + (isMonitorSelected(row.id) ? 'checked="checked"' : '') + '/></td>' +
        '<td><span style="display:inline-block;width:12px;height:12px;background:' + c + ';border-radius:50%;margin-right:6px;"></span>' + escapeHtml(row.id) + '</td>' +
        '<td>' + escapeHtml(row.host) + '</td>' +
        '<td>' +
          '<span class="' + changeClass(prevVals.totalMemoryMB, currVals.totalMemoryMB) + '">' + fmtMB(row.totalMemoryMB) + (row.totalMemorySource === 'rss' ? ' <small style="color:#888;">(rss)</small>' : '') + '</span>' +
          ' / ' +
          '<span class="' + changeClass(prevVals.heapMaxMB, currVals.heapMaxMB) + '">' + fmtMB(row.heapMaxMB) + '</span>' +
        '</td>' +
        '<td' + heapPctStyle + '>' +
          '<span class="' + changeClass(prevVals.heapMB, currVals.heapMB) + '">' + fmtMB(row.heapMB) + '</span>' +
          ' / ' +
          '<span class="' + changeClass(prevVals.heapPct, currVals.heapPct) + '">' + fmtPct(row.heapPct) + '</span>' +
        '</td>' +
        '<td>' +
          '<span class="' + changeClass(prevVals.jvmOffHeapMB, currVals.jvmOffHeapMB) + '">' + fmtMB(row.jvmOffHeapMB) + '</span>' +
          ' / ' +
          '<span class="' + changeClass(prevVals.jvmOffHeapPct, currVals.jvmOffHeapPct) + '">' + fmtPct(currVals.jvmOffHeapPct) + '</span>' +
        '</td>' +
        '<td>' +
          '<span class="' + changeClass(prevVals.offHeapUnifiedMB, currVals.offHeapUnifiedMB) + '">' + fmtMB(row.offHeapUnifiedMB) + '</span>' +
          ' / ' +
          '<span class="' + changeClass(prevVals.offHeapUnifiedPct, currVals.offHeapUnifiedPct) + '">' + fmtPct(currVals.offHeapUnifiedPct) + '</span>' +
        '</td>' +
        '<td>' +
          '<span class="' + changeClass(prevVals.nativeOtherMB, currVals.nativeOtherMB) + '">' + (row.nativeOtherMB != null ? fmtMB(row.nativeOtherMB) : '-') + '</span>' +
          ' / ' +
          '<span class="' + changeClass(prevVals.nativeOtherPct, currVals.nativeOtherPct) + '">' + fmtPct(currVals.nativeOtherPct) + '</span>' +
        '</td>' +
        tdWithChange(fmtSeconds(row.gcSeconds), '', changeClass(prevVals.gcSeconds, currVals.gcSeconds)) +
        tdWithChange(fmtGcDelta(row.gcDeltaMs), gcDeltaStyle, changeClass(prevVals.gcDeltaMs, currVals.gcDeltaMs)) +
        tdWithChange(fmtInt(row.activeTasks), '', changeClass(prevVals.activeTasks, currVals.activeTasks)) +
        tdWithChange(fmtInt(row.maxTasks), '', changeClass(prevVals.maxTasks, currVals.maxTasks)) +
        tdWithChange(fmtPct(row.saturationPct), saturationStyle, changeClass(prevVals.saturationPct, currVals.saturationPct)) +
        tdWithChange(fmtPct(row.cpuPct) + (row.cpuSource === 'totalDuration' ? ' <small style="color:#8a6d3b;font-size:10px;" title="cpuTimeMs non disponibile: stima basata su totalDuration (wall-clock), non CPU reale.">(est.)</small>' : (row.cpuSource === 'OS' ? ' <small style="color:#3c763d;font-size:10px;" title="CPU reale e live, letta dal SO tramite ProcessCpuPlugin (spark.plugins).">(OS)</small>' : '')), cpuStyle, changeClass(prevVals.cpuPct, currVals.cpuPct)) +
        tdWithChange(fmtInt(row.failedTasks), failedTasksStyle, changeClass(prevVals.failedTasks, currVals.failedTasks)) +
        tdWithChange(fmtMB(row.shuffleReadMB), '', changeClass(prevVals.shuffleReadMB, currVals.shuffleReadMB)) +
        tdWithChange(fmtMB(row.shuffleWriteMB), '', changeClass(prevVals.shuffleWriteMB, currVals.shuffleWriteMB)) +
        tdWithChange(fmtInt(row.rddBlocks), '', changeClass(prevVals.rddBlocks, currVals.rddBlocks)) +
        tdWithChange(fmtMB(row.diskUsedMB), '', changeClass(prevVals.diskUsedMB, currVals.diskUsedMB)) +
        tdWithChange(fmtMB(row.memoryUsedMB), '', changeClass(prevVals.memoryUsedMB, currVals.memoryUsedMB)) +
        tdWithChange(fmtMB(row.onHeapStorageMB), '', changeClass(prevVals.onHeapStorageMB, currVals.onHeapStorageMB)) +
        tdWithChange(fmtMB(row.totalOnHeapCapacityMB), '', changeClass(prevVals.totalOnHeapCapacityMB, currVals.totalOnHeapCapacityMB)) +
        tdWithChange(fmtMB(row.onHeapExecMB), '', changeClass(prevVals.onHeapExecMB, currVals.onHeapExecMB)) +
        tdWithChange(fmtMB(row.onHeapStoragePeakMB), '', changeClass(prevVals.onHeapStoragePeakMB, currVals.onHeapStoragePeakMB)) +
        tdWithChange(fmtMB(row.onHeapUnifiedMB), '', changeClass(prevVals.onHeapUnifiedMB, currVals.onHeapUnifiedMB)) +
        '</tr>';
    });
    prevTableValuesByExecutor = nextTableValuesByExecutor;
    updateSelectedExecInfo(rows);
  }
  function renderStatus(rows) {
    var status = document.getElementById('heapStatus');
    var apiStatus = document.getElementById('heapApiStatus');
    var unhealthyCount = rows.filter(function(row) { return row.isUnhealthy; }).length;
    if (rows.length === 0) {
      if (getMonitorMode() === 'selected') {
        status.textContent = 'No executors selected for charts with current filters.';
      } else {
        status.textContent = document.getElementById('onlyUnhealthy').checked
          ? 'No unhealthy executors with current filters.'
          : 'No active executors found.';
      }
      status.style.color = '#a94442';
    } else if (latestWithHeap === 0) {
      status.textContent = 'Executors found but heap metric is not available from the REST API (check executorMetrics).';
      status.style.color = '#8a6d3b';
    } else {
      var totalFailed = rows.reduce(function(sum, row) { return sum + (row.failedTasks || 0); }, 0);
      var totalActiveTasks = rows.reduce(function(sum, row) { return sum + (row.activeTasks || 0); }, 0);
      status.textContent = 'Shown executors: ' + rows.length + ' | unhealthy: ' + unhealthyCount + ' | heap available for ' + latestWithHeap + ' | active tasks: ' + totalActiveTasks + ' | failed tasks: ' + totalFailed + '.';
      status.style.color = unhealthyCount > 0 ? '#8a6d3b' : '#3c763d';
    }
    apiStatus.textContent = 'Metrics endpoint: ' + latestEndpoint + ' | records: ' + latestRecordCount + ' | sort: ' + sortState.key + ' ' + sortState.direction;
  }
  function rerenderFromState() {
    var warnPctRaw = toNum(document.getElementById('warnPct').value);
    var warnPct = warnPctRaw != null ? warnPctRaw : 85;
    var satWarnPctRaw = toNum(document.getElementById('satWarnPct').value);
    var satWarnPct = satWarnPctRaw != null ? satWarnPctRaw : 400;
    // Always keep live snapshot recording up to date
    if (latestExecutorPayload.length > 0) {
      latestRows = latestExecutorPayload.map(function(ex) { return toRow(ex, warnPct, satWarnPct); });
      recordRowsSnapshot(latestRows);
      latestWithHeap = latestRows.filter(function(row) { return row.heapB != null; }).length;
    }
    var onlyUnhealthy = !!document.getElementById('onlyUnhealthy').checked;
    if (viewRangeMinutes !== null) {
      // ── History view ─────────────────────────────────────────────
      buildHistoryView(viewRangeMinutes);
      var hRows = histViewRows.slice();
      if (onlyUnhealthy) hRows = hRows.filter(function(r) { return r.isUnhealthy; });
      latestVisibleRows = hRows;
      var hRowsForCharts = applyMonitorSelection(hRows);
      updateSortHeaders();
      // Swap globals to historical data for chart/legend rendering
      var savedLabels = labels; var savedExecutors = executors;
      labels = histViewLabels; executors = histViewExecutors;
      renderCharts(hRowsForCharts);
      renderLegend();
      labels = savedLabels; executors = savedExecutors;
      renderSummary(hRowsForCharts);
      renderTopOffenders(hRowsForCharts);
      renderTable(hRows, warnPct, satWarnPct);
      renderStatus(hRowsForCharts);
      updateSelectedExecInfo(hRows);
      return;
    }
    // ── Live session view ─────────────────────────────────────────
    var visibleRows = latestRows.slice();
    if (onlyUnhealthy) visibleRows = visibleRows.filter(function(row) { return row.isUnhealthy; });
    latestVisibleRows = visibleRows;
    var rowsForCharts = applyMonitorSelection(visibleRows);
    var usingPeak = latestRows.some(function(row) { return row.metricSource && row.metricSource.indexOf('peakMemoryMetrics') >= 0; });
    var peakWarning = document.getElementById('heapPeakWarning');
    if (peakWarning) peakWarning.style.display = usingPeak ? 'block' : 'none';
    updateSortHeaders();
    renderCharts(rowsForCharts);
    renderLegend();
    renderSummary(rowsForCharts);
    renderTopOffenders(rowsForCharts);
    renderTable(visibleRows, warnPct, satWarnPct);
    renderStatus(rowsForCharts);
    updateSelectedExecInfo(visibleRows);
  }

  /* ── poll ──────────────────────────────────────────────────────── */
  function fetchExecutors() {
    // Same-origin endpoint served directly from the driver's AppStatusStore (see
    // ChartsPage.renderJson) — no outbound HTTP request to the public REST API, so this stays
    // reliable behind reverse proxies (YARN ResourceManager proxy, Knox, ...) that may otherwise
    // interfere with calls to /api/v1/....
    return fetch(METRICS_URL)
      .then(function(r) {
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json().then(function(data) { return { data: data, endpoint: METRICS_URL }; });
      });
  }

  function poll() {
    fetchExecutors()
      .then(function(payload) {
        // The endpoint returns both active AND already-terminated executors (see
        // ChartsPage.renderJson). Once dead, an executor's metrics are frozen at their last
        // known values (Spark keeps its final snapshot around for the History Server), so they
        // must be excluded from the *live* series/table/summary — otherwise a dead executor
        // would appear to permanently "hold" its last memory reading forever. Dead executors'
        // samples recorded while they were alive remain untouched in `historyRows`.
        var exList = payload.data.filter(function(ex) { return ex.isActive; });
        latestExecutorPayload = exList;
        latestEndpoint = payload.endpoint;
        latestRecordCount = exList.length;

        /* push time label */
        pollSeq++;
        var t = nowLabel();
        if (labels.length >= MAX_POINTS) labels.shift();
        labels.push(t);
        latestPollLabel = t;

        /* add a null sample to all known executors (keeps dataset lengths aligned) */
        Object.keys(executors).forEach(function(id) {
          var series = executors[id];
          if (!Array.isArray(series.overheadData)) series.overheadData = [];
          if (!Array.isArray(series.offHeapData)) series.offHeapData = [];
          if (!Array.isArray(series.totalMemoryData)) series.totalMemoryData = [];
          if (!Array.isArray(series.rssData)) series.rssData = [];
          if (!Array.isArray(series.cpuData)) series.cpuData = [];
          [series.heapData, series.overheadData, series.offHeapData, series.totalMemoryData, series.rssData, series.gcData, series.saturationData, series.cpuData].forEach(function(dsAll) {
            if (dsAll.length >= MAX_POINTS) dsAll.shift();
            dsAll.push(null);
          });
        });

        /* update per-executor dataset */
        exList.forEach(function(ex) {
          var id      = String(ex.id);
          var currentReading = heapCurrentOf(ex);
          var heapB   = currentReading.bytes;
          var heapMB  = heapB != null ? toMB(heapB) : null;
          var series = ensureExecutorSeries(id);
          series.heapData[series.heapData.length - 1] = heapMB != null ? parseFloat(heapMB.toFixed(2)) : null;
          var jvmOffHeapB = ex.peakMemoryMetrics ? toNum(ex.peakMemoryMetrics.JVMOffHeapMemory) : null;
          var jvmOffHeapMB = jvmOffHeapB != null ? toMB(jvmOffHeapB) : null;
          var offHeapUnifiedB = ex.peakMemoryMetrics ? toNum(ex.peakMemoryMetrics.OffHeapUnifiedMemory) : null;
          var offHeapUnifiedMB = offHeapUnifiedB != null ? toMB(offHeapUnifiedB) : null;
          var totalMemoryMB = totalMemoryMBOf(heapMB, jvmOffHeapMB, offHeapUnifiedMB);
          // Real OS-level RSS of the JVM process tree — only populated when the cluster has
          // spark.executor.processTreeMetrics.enabled=true (and /proc is readable); see
          // resolveTotalMemory() for how this feeds the "Total" line/column depending on the
          // user-selected totalMemoryMode.
          var rssB = ex.peakMemoryMetrics ? toNum(ex.peakMemoryMetrics.ProcessTreeJVMRSSMemory) : null;
          var rssMB = rssB != null ? toMB(rssB) : null;
          if (rssMB != null) processTreeMetricsAvailable = true;
          series.overheadData[series.overheadData.length - 1] = jvmOffHeapMB != null ? parseFloat(jvmOffHeapMB.toFixed(2)) : null;
          series.offHeapData[series.offHeapData.length - 1] = offHeapUnifiedMB != null ? parseFloat(offHeapUnifiedMB.toFixed(2)) : null;
          series.totalMemoryData[series.totalMemoryData.length - 1] = totalMemoryMB != null ? parseFloat(totalMemoryMB.toFixed(2)) : null;
          series.rssData[series.rssData.length - 1] = rssMB != null ? parseFloat(rssMB.toFixed(2)) : null;

          // GC delta in ms: GC time consumed in the last interval
          var curGcMs = toNum(ex.totalGCTime);
          var curDuration = toNum(ex.totalDuration);
          var gcDeltaMs = gcDeltaMsCompute(id, curGcMs);
          series.gcData[series.gcData.length - 1] = gcDeltaMs != null ? parseFloat(gcDeltaMs.toFixed(1)) : null;
          series.lastGcDeltaMs = gcDeltaMs;
          // save state for next delta
          if (curGcMs != null) {
            gcPrevState[id] = { gcMs: curGcMs, duration: curDuration };
          }

          var saturationPct = saturationPctOf(ex);
          series.saturationData[series.saturationData.length - 1] = saturationPct != null ? parseFloat(saturationPct.toFixed(2)) : null;

          // CPU% delta with double fallback
          var curWallMs = Date.now();
          var cpuResult = cpuDeltaCompute(id, ex, curWallMs);
          var cpuPct = cpuResult.pct;
          series.cpuData[series.cpuData.length - 1] = cpuPct != null ? parseFloat(Math.min(cpuPct, 200).toFixed(2)) : null;
          series.lastCpuPct = cpuPct;
          series.lastCpuSource = cpuResult.source;
          // save state for next delta
          cpuPrevState[id] = { cpuMs: toNum(ex.cpuTimeMs), totalDuration: toNum(ex.totalDuration), wallMs: curWallMs };
        });

        document.getElementById('lastUpdate').textContent =
          'Last update: ' + new Date().toLocaleTimeString();
        updateRssAvailabilityWarning();
        rerenderFromState();
      })
      .catch(function(e) {
        var status = document.getElementById('heapStatus');
        var apiStatus = document.getElementById('heapApiStatus');
        status.textContent = 'Metrics polling error: ' + e;
        status.style.color = '#a94442';
        apiStatus.textContent = 'Metrics endpoint: ' + METRICS_URL;
        console.error('[Charts] poll error:', e);
      });
  }

  /* ── interval control ──────────────────────────────────────────── */
  function startPolling() {
    if (intervalId) clearInterval(intervalId);
    intervalId = setInterval(poll, pollMs);
  }

  window.updateInterval = function() {
    var v = parseInt(document.getElementById('pollInterval').value, 10);
    if (v > 0) { pollMs = v * 1000; startPolling(); }
  };

  window.applyOptions = function() {
    if (latestExecutorPayload.length > 0) rerenderFromState();
    else poll();
  };

  var monitorAllCheckbox = document.getElementById('monitorAllCheckbox');
  if (monitorAllCheckbox) {
    monitorAllCheckbox.addEventListener('change', function() {
      var checked = !!monitorAllCheckbox.checked;
      latestVisibleRows.forEach(function(row) { selectedExecutors[row.id] = checked; });
      rerenderFromState();
    });
  }
  var selectAllVisibleBtn = document.getElementById('selectAllVisibleExec');
  if (selectAllVisibleBtn) {
    selectAllVisibleBtn.addEventListener('click', function() {
      latestVisibleRows.forEach(function(row) { selectedExecutors[row.id] = true; });
      rerenderFromState();
    });
  }
  var clearSelectedBtn = document.getElementById('clearSelectedExec');
  if (clearSelectedBtn) {
    clearSelectedBtn.addEventListener('click', function() {
      selectedExecutors = {};
      rerenderFromState();
    });
  }
  var resetLayoutNode = document.getElementById('resetLayoutBtn');
  if (resetLayoutNode) {
    resetLayoutNode.addEventListener('click', function() {
      resetLayoutControls();
    });
  }
  var heapTableNode = document.getElementById('heapTable');
  if (heapTableNode) {
    heapTableNode.addEventListener('change', function(evt) {
      var t = evt.target;
      if (!t || !t.classList || !t.classList.contains('monitor-exec-checkbox')) return;
      var id = t.getAttribute('data-exec-id');
      if (!id) return;
      selectedExecutors[id] = !!t.checked;
      if (getMonitorMode() === 'selected') rerenderFromState();
      else updateSelectedExecInfo(latestVisibleRows);
    });
  }

  var recordHistoryNode = document.getElementById('recordHistory');
  if (recordHistoryNode) {
    recordHistoryNode.addEventListener('change', function() {
      updateHistoryStatus();
    });
  }
  var downloadHistoryNode = document.getElementById('downloadHistory');
  if (downloadHistoryNode) {
    downloadHistoryNode.addEventListener('click', function() {
      downloadHistoryCsv();
    });
  }
  var resetHistoryNode = document.getElementById('resetHistory');
  if (resetHistoryNode) {
    resetHistoryNode.addEventListener('click', function() {
      resetHistory();
    });
  }

  tableHeaders.forEach(function(header) {
    header.addEventListener('click', function() {
      var key = header.getAttribute('data-sort-key');
      if (sortState.key === key) {
        sortState.direction = sortState.direction === 'asc' ? 'desc' : 'asc';
      } else {
        sortState.key = key;
        sortState.direction = key === 'id' || key === 'host' ? 'asc' : 'desc';      }
      rerenderFromState();
    });
  });

  window.addEventListener('resize', function() {
    // Keep chart state consistent in both live mode and history mode after resize.
    rerenderFromState();
  });

  Array.prototype.forEach.call(document.querySelectorAll('.heap-vr-btn'), function(btn) {
    btn.addEventListener('click', function() {
      var vr = btn.getAttribute('data-vr');
      if (vr === 'session') setViewRange(null);
      else if (vr === 'all') setViewRange(Infinity);
      else setViewRange(Number(vr));
    });
  });

  /* ── kick off ──────────────────────────────────────────────────── */
  loadLayoutState();
  applyLayoutState();
  loadTotalMemoryMode();
  var initialRssRadio = document.getElementById('totalMemoryModeRss');
  var initialEstRadio = document.getElementById('totalMemoryModeEstimated');
  if (initialRssRadio) initialRssRadio.checked = totalMemoryMode === 'rss';
  if (initialEstRadio) initialEstRadio.checked = totalMemoryMode === 'estimated';
  loadHistory();
  if (!localStorageAvailable) {
    // Persistence across page reloads isn't available in this context (e.g. storage blocked
    // behind a reverse proxy), but recording/CSV export/reset still work against the in-memory
    // buffer for the lifetime of this tab — so none of the history controls are disabled here.
    // See the "no storage" indicator rendered by updateHistoryStatus().
    console.warn('[Charts] localStorage not available — history will be kept in memory only ' +
      'for this session (no persistence across page reloads).');
  }
  poll();
  startPolling();
})();
"""
}

object ChartsPage {
  /** Default polling interval in **seconds** (rendered as the input default value). */
  val PollIntervalSeconds: Int = 5
  /** Maximum number of data points kept per executor in the rolling window. */
  val MaxPoints: Int = 120
}

/**
 * Wraps a plain `ExecutorSummary` with the real, cumulative per-executor CPU time (see
 * [[ExecutorCpuTimeListener]]) that Spark's own REST API doesn't expose. `@JsonUnwrapped` flattens
 * `summary`'s own fields directly onto this object at serialization time, so each element of
 * `ChartsPage.renderJson`'s JSON array still looks like a plain `ExecutorSummary` object — just
 * with a couple of extra sibling fields — instead of a nested `{"summary": {...}, ...}` shape.
 *
 * @param osCpuLoadPct optional, truly live OS-level process CPU% (see [[ProcessCpuPlugin]]);
 *                     `None` unless the plugin is explicitly enabled via `spark.plugins`.
 */
private[ui] case class ExecutorSummaryWithCpuTime(
    @JsonUnwrapped summary: ExecutorSummary,
    cpuTimeMs: Long,
    osCpuLoadPct: Option[Double] = None)

