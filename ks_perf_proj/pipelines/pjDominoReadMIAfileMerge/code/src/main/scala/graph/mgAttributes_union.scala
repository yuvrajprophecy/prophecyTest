package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object mgAttributes_union {

  def apply(
    context:         Context,
    lkMergeOrigData: DataFrame,
    lkSessionID:     DataFrame,
    lkTimeStamp:     DataFrame,
    lkPageName:      DataFrame,
    lkPortletName:   DataFrame,
    lkAccount:       DataFrame,
    lkAction:        DataFrame,
    lkCustId:        DataFrame,
    lkID:            DataFrame,
    lkStaffNum:      DataFrame,
    lkChanlId:       DataFrame,
    lkAppNum:        DataFrame,
    lkGlblSessnId:   DataFrame
  ): DataFrame =
    List(lkMergeOrigData,
         lkSessionID,
         lkTimeStamp,
         lkPageName,
         lkPortletName,
         lkAccount,
         lkAction,
         lkCustId,
         lkID,
         lkStaffNum,
         lkChanlId,
         lkAppNum,
         lkGlblSessnId
    ).flatMap(Option(_)).reduce(_.unionAll(_))

}
