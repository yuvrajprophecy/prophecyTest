package graph

import io.prophecy.libs._
import udfs.UDFs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object RTR_Reject_Recs {

  case class RowDistributorProperties(
    @Property("", "") outports: List[FileTab] = Nil
  ) extends ComponentProperties

  @Property("Helper") case class FileTab(
    @Property("path", "") path: String,
    @Property("id", "") id: String,
    @Property("model", "") model: SColumn
  )

  def apply(context: Context, in: DataFrame): (DataFrame, DataFrame) = {
    val spark  = context.spark
    val Config = context.config
    val props = RowDistributorProperties(outports =
      List(
        FileTab(path = "out0",
                id = "RTR_Reject_Recs__out0",
                model = SColumn("lit(true)")
        )
      )
    )
    val first :: second :: rest = props.outports.map { outport =>
      in.filter(outport.model.column)
    }
    val (out1, out0, List()) = (first, second, rest)
    (out1, out0)
  }

}
