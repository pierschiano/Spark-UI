package org.apache.spark.ui.support

import org.apache.spark.{SparkConf, SparkContext}

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import javax.servlet.http.HttpServletRequest

/**
 * Shared helpers for testing the web-ui module without requiring a real servlet container or a
 * running cluster:
 *
 *   - [[withLocalSparkContext]] spins up a local, UI-enabled `SparkContext` for the duration of a
 *     test and guarantees it is stopped afterwards (each test gets its own context, since only one
 *     `SparkContext` may be active per JVM at a time).
 *   - [[fakeRequest]] builds a minimal `HttpServletRequest` double via a JDK dynamic proxy — enough
 *     to satisfy `UIUtils.prependBaseUri`/`headerSparkPage`, which only call `getHeader` on it.
 */
trait SparkUiTestSupport {

  /** Runs `body` against a fresh local `SparkContext` with the Spark UI enabled on a random port. */
  def withLocalSparkContext[T](appName: String = "hpsc-web-ui-test")(body: SparkContext => T): T = {
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName(appName)
      .set("spark.ui.enabled", "true")
      .set("spark.ui.port", "0")
      .set("spark.driver.host", "localhost")
    val sc = new SparkContext(conf)
    try body(sc) finally sc.stop()
  }

  /** Runs `body` against a fresh local `SparkContext` with the Spark UI disabled. */
  def withUiDisabledSparkContext[T](appName: String = "hpsc-web-ui-test-no-ui")(body: SparkContext => T): T = {
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName(appName)
      .set("spark.ui.enabled", "false")
      .set("spark.driver.host", "localhost")
    val sc = new SparkContext(conf)
    try body(sc) finally sc.stop()
  }

  /**
   * A dynamic-proxy `HttpServletRequest` stub: every method returns a JVM-default value (`null`,
   * `0`, `false`, ...) except `getHeader`, which always returns `null` (i.e. "no reverse-proxy
   * headers present"). This is sufficient for `UIUtils`, which only reads `getHeader` when
   * resolving the UI base path.
   */
  def fakeRequest(): HttpServletRequest = {
    val handler = new InvocationHandler {
      override def invoke(proxy: scala.Any, method: Method, args: Array[AnyRef]): AnyRef = {
        method.getName match {
          case "getHeader" => null
          case "toString" => "FakeHttpServletRequest"
          case "hashCode" => Int.box(System.identityHashCode(proxy))
          case "equals" =>
            java.lang.Boolean.valueOf(args != null && args.nonEmpty && proxy.asInstanceOf[AnyRef].eq(args(0)))
          case _ =>
            val returnType = method.getReturnType
            if (returnType == java.lang.Boolean.TYPE) java.lang.Boolean.FALSE
            else if (returnType == java.lang.Integer.TYPE) Int.box(0)
            else if (returnType == java.lang.Long.TYPE) Long.box(0L)
            else null
        }
      }
    }
    Proxy.newProxyInstance(
      getClass.getClassLoader,
      Array(classOf[HttpServletRequest]),
      handler
    ).asInstanceOf[HttpServletRequest]
  }

}
