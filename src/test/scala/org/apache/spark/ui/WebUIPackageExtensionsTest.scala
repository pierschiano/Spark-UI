package org.apache.spark.ui

import it.eng.spark.hpsc.webui._
import org.apache.spark.ui.support.SparkUiTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Tests the public API in `it.eng.spark.hpsc.webui` — the implicit `SparkContext` extensions
 * that end users import (`sc.attachUITab`, `sc.attachUIPage`, `sc.attachChartsTab`). Verifies that
 * they delegate correctly to [[HpscUI]] (tab/page creation, idempotency, title defaulting).
 *
 * This suite deliberately lives under `org.apache.spark.ui` (rather than mirroring the
 * `it.eng.spark.hpsc.webui` package) so it can inspect `sc.ui`/`SparkUI`, which are
 * `private[spark]`.
 */
class WebUIPackageExtensionsTest extends AnyFlatSpec with Matchers with SparkUiTestSupport {

  "sc.attachUITab" should "attach a tab reachable at its own tabName" in withLocalSparkContext() { sc =>
    sc.attachUITab("my-tab") { _ => <div>content</div> }

    sc.ui.get.getTabs.map(_.prefix) should contain("my-tab")
  }

  it should "default the page title to the capitalized tabName" in withLocalSparkContext() { sc =>
    sc.attachUITab("my-tab") { _ => <div>content</div> }

    val tab = sc.ui.get.getTabs.find(_.prefix == "my-tab").getOrElse(fail("tab not found"))
    val rendered = tab.pages.head.render(fakeRequest())
    (rendered \\ "title").text should include("My-tab")
  }

  it should "use an explicit title when provided" in withLocalSparkContext() { sc =>
    sc.attachUITab("my-tab", title = "Custom Title") { _ => <div>content</div> }

    val tab = sc.ui.get.getTabs.find(_.prefix == "my-tab").getOrElse(fail("tab not found"))
    val rendered = tab.pages.head.render(fakeRequest())
    (rendered \\ "title").text should include("Custom Title")
  }

  it should "be idempotent for repeated calls with the same tabName" in withLocalSparkContext() { sc =>
    sc.attachUITab("idempotent-tab") { _ => <div>first</div> }
    sc.attachUITab("idempotent-tab") { _ => <div>second</div> }

    sc.ui.get.getTabs.count(_.prefix == "idempotent-tab") shouldEqual 1
  }

  "sc.attachUIPage" should "create the tab on first use and attach the page under it" in withLocalSparkContext() { sc =>
    sc.attachUIPage("my-tab", "details") { _ => <div>details-content</div> }

    val tab = sc.ui.get.getTabs.find(_.prefix == "my-tab").getOrElse(fail("tab not found"))
    tab.pages.map(_.prefix) should contain("my-tab/details")
  }

  it should "default the page title to the capitalized pagePrefix" in withLocalSparkContext() { sc =>
    sc.attachUIPage("my-tab", "details") { _ => <div>details-content</div> }

    val tab = sc.ui.get.getTabs.find(_.prefix == "my-tab").getOrElse(fail("tab not found"))
    val page = tab.pages.find(_.prefix == "my-tab/details").getOrElse(fail("page not found"))
    (page.render(fakeRequest()) \\ "title").text should include("Details")
  }

  it should "allow attaching multiple pages under the same tab" in withLocalSparkContext() { sc =>
    sc.attachUIPage("multi-tab", "page-a") { _ => <div>a</div> }
    sc.attachUIPage("multi-tab", "page-b") { _ => <div>b</div> }

    val tab = sc.ui.get.getTabs.find(_.prefix == "multi-tab").getOrElse(fail("tab not found"))
    tab.pages.map(_.prefix) should contain allOf("multi-tab/page-a", "multi-tab/page-b")
  }

  "sc.attachChartsTab" should "attach a tab named 'charts' by default" in withLocalSparkContext() { sc =>
    sc.attachChartsTab()

    sc.ui.get.getTabs.map(_.prefix) should contain("charts")
  }

  it should "attach a tab with a custom name when provided" in withLocalSparkContext() { sc =>
    sc.attachChartsTab(tabName = "my-metrics")

    sc.ui.get.getTabs.map(_.prefix) should contain("my-metrics")
  }

  it should "be a no-op when called twice with the same tabName" in withLocalSparkContext() { sc =>
    sc.attachChartsTab()
    sc.attachChartsTab()

    sc.ui.get.getTabs.count(_.prefix == "charts") shouldEqual 1
  }

}
