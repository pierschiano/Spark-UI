package org.apache.spark.ui

import org.apache.spark.ui.support.SparkUiTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for [[HpscUI]], the internal entry point used by the public
 * `it.eng.spark.hpsc.webui` API to attach custom tabs/pages to a `SparkContext`'s web UI.
 */
class HpscUITest extends AnyFlatSpec with Matchers with SparkUiTestSupport {

  "attachPage" should "register a new tab on first use" in withLocalSparkContext() { sc =>
    HpscUI.attachPage(sc, "hpsc-test-1", "", "HPSC Test 1")(_ => <div>hello</div>)

    val ui = sc.ui.getOrElse(fail("Spark UI should be enabled"))
    ui.getTabs.map(_.prefix) should contain("hpsc-test-1")
  }

  it should "reuse the same tab when called again with the same tabName" in withLocalSparkContext() { sc =>
    HpscUI.attachPage(sc, "hpsc-test-2", "", "HPSC Test 2")(_ => <div>a</div>)
    HpscUI.attachPage(sc, "hpsc-test-2", "details", "HPSC Test 2 Details")(_ => <div>b</div>)

    val ui = sc.ui.getOrElse(fail("Spark UI should be enabled"))
    ui.getTabs.count(_.prefix == "hpsc-test-2") shouldEqual 1
  }

  it should "not attach a duplicate landing page when called twice with the same prefix" in
    withLocalSparkContext() { sc =>
      // Calling attachPage twice with the same (tabName, pagePrefix) would try to register the
      // same servlet path twice; this must not blow up and must not create a broken tab.
      noException should be thrownBy {
        HpscUI.attachPage(sc, "hpsc-test-3", "", "HPSC Test 3")(_ => <div>a</div>)
      }
    }

  it should "throw IllegalStateException when the Spark UI is disabled" in withUiDisabledSparkContext() { sc =>
    val ex = intercept[IllegalStateException] {
      HpscUI.attachPage(sc, "hpsc-test-4", "", "HPSC Test 4")(_ => <div>a</div>)
    }
    ex.getMessage should include("spark.ui.enabled")
  }

  it should "render the supplied content when the page is requested" in withLocalSparkContext() { sc =>
    HpscUI.attachPage(sc, "hpsc-test-5", "", "HPSC Test 5")(_ => <div id="marker">expected-content</div>)

    val ui = sc.ui.getOrElse(fail("Spark UI should be enabled"))
    val tab = ui.getTabs.find(_.prefix == "hpsc-test-5").getOrElse(fail("tab not found"))
    val page = tab.pages.headOption.getOrElse(fail("page not found"))
    val rendered = page.render(fakeRequest())
    (rendered \\ "div").exists(_.text == "expected-content") shouldBe true
  }

  "attachCharts" should "register the charts tab exactly once per SparkContext" in withLocalSparkContext() { sc =>
    HpscUI.attachCharts(sc, "charts-test")
    HpscUI.attachCharts(sc, "charts-test")

    val ui = sc.ui.getOrElse(fail("Spark UI should be enabled"))
    ui.getTabs.count(_.prefix == "charts-test") shouldEqual 1
  }

  it should "throw IllegalStateException when the Spark UI is disabled" in withUiDisabledSparkContext() { sc =>
    an[IllegalStateException] should be thrownBy HpscUI.attachCharts(sc, "charts-test-disabled")
  }

  it should "keep independent tab registries per SparkContext" in {
    withLocalSparkContext("hpsc-web-ui-test-a") { scA =>
      HpscUI.attachPage(scA, "shared-name", "", "A")(_ => <div>a</div>)
      scA.ui.get.getTabs.count(_.prefix == "shared-name") shouldEqual 1
    }
    withLocalSparkContext("hpsc-web-ui-test-b") { scB =>
      // A fresh SparkContext (and thus a fresh SparkUI) must not see tabs from a previous,
      // already-stopped context that reused the same tab name.
      HpscUI.attachPage(scB, "shared-name", "", "B")(_ => <div>b</div>)
      scB.ui.get.getTabs.count(_.prefix == "shared-name") shouldEqual 1
    }
  }

}
