package graph

import io.prophecy.libs._
import config.Context
import udfs.UDFs._
import udfs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object EP_Generate_CRC {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
      val out = in.select(
        crc32(
          concat(
            col("INFIELD"),
            col("INFIELD1"),
            col("INFIELD2"),
            col("INFIELD3"),
            col("INFIELD4"),
            col("INFIELD5"),
            col("INFIELD6"),
            col("INFIELD7"),
            col("INFIELD8"),
            col("INFIELD9"),
            col("INFIELD10"),
            col("INFIELD11"),
            col("INFIELD12"),
            col("INFIELD13"),
            col("INFIELD14"),
            col("INFIELD15"),
            col("INFIELD16"),
            col("INFIELD17"),
            col("INFIELD18"),
            col("INFIELD19"),
            col("INFIELD20"),
            col("INFIELD21"),
            col("INFIELD22"),
            col("INFIELD23"),
            col("INFIELD24"),
            col("INFIELD25"),
            col("INFIELD26"),
            col("INFIELD27"),
            col("INFIELD28"),
            col("INFIELD29"),
            col("INFIELD30"),
            col("INFIELD31"),
            col("INFIELD32"),
            col("INFIELD33"),
            col("INFIELD34"),
            col("INFIELD35"),
            col("INFIELD36"),
            col("INFIELD37"),
            col("INFIELD38"),
            col("INFIELD39"),
            col("INFIELD40"),
            col("INFIELD41"),
            col("INFIELD42"),
            col("INFIELD43"),
            col("INFIELD44"),
            col("INFIELD45"),
            col("INFIELD46"),
            col("INFIELD47"),
            col("INFIELD48"),
            col("INFIELD49")
          )
        ).as("CRC_NUM")
      )
    out
  }

}
