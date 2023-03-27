package io.prophecy.pipelines.basepipeline1.graph.Subgraph_1

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import io.prophecy.pipelines.basepipeline1.config._
import io.prophecy.libs.registerAllUDFs
import io.prophecy.libs.SparkTestingUtils._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.junit.runner.RunWith
import org.junit.Assert
import org.scalatest.FunSuite
import java.time.LocalDateTime
import org.scalatest.junit.JUnitRunner
import java.sql.{Date, Timestamp}
import scala.collection.JavaConverters._
import java.nio.file.{Files, Paths}
import java.math.BigDecimal

@RunWith(classOf[JUnitRunner])
class Reformat_1Test extends FunSuite with DataFrameSuiteBase {
  import sqlContext.implicits._
  var context: Context = null

  test("Unit Test 0") {

    val dfIn = createDfFromResourceFiles(
      spark,
      "/data/io/prophecy/pipelines/basepipeline1/graph/Subgraph_1/Reformat_1/in/schema.json",
      "/data/io/prophecy/pipelines/basepipeline1/graph/Subgraph_1/Reformat_1/in/data/unit_test_0.json",
      "in"
    )
    val dfOut = createDfFromResourceFiles(
      spark,
      "/data/io/prophecy/pipelines/basepipeline1/graph/Subgraph_1/Reformat_1/out/schema.json",
      "/data/io/prophecy/pipelines/basepipeline1/graph/Subgraph_1/Reformat_1/out/data/unit_test_0.json",
      "out"
    )

    val dfOutComputed =
      io.prophecy.pipelines.basepipeline1.graph.Subgraph_1.Reformat_1(
        io.prophecy.pipelines.basepipeline1.graph.Subgraph_1.config
          .Context(context.spark, context.config.Subgraph_1),
        dfIn
      )

  }

  override def beforeAll() = {
    super.beforeAll()
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerAllUDFs(spark)

    val fabricName = System.getProperty("fabric")

    val config = ConfigurationFactoryImpl.fromCLI(
      Array("--confFile",
            getClass.getResource(s"/config/${fabricName}.json").getPath
      )
    )

    context = Context(spark, config)
  }

}
