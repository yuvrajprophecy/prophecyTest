package graph

import io.prophecy.libs._
import config.Context
import udfs.UDFs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object swSplitData {
  def apply(context: Context, lkSplitData: DataFrame): (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {
    val spark = context.spark
    val Config = context.config
    
     // Could not generate the component
     // Name : swSplitData
     // Properties :
     /*
     Name -> Some(swSplitData)
    NextRecordID -> Some(0)
    AllowColumnMapping -> Some(0)
    NextID -> Some(46)
    InputPins -> Some(V65S5P1)
    key -> Some(\(2)\(2)0\(1)\(3)key\(2)ATTRIB_NAME\(2)0)
    hashSelector -> None
    selection -> Some(user)
    ifNotFound -> Some(allow)
    OutputPins -> Some(V65S5P21|V65S5P22|V65S5P24|V65S5P25|V65S5P26|V65S5P27|V65S5P40|V65S5P41|V65S5P42|V65S5P43|V65S5P44|V65S5P45|V65S5P28)
    StageType -> Some(PxSwitch)
    case -> Some(\(2)\(2)0\(1)\(3)case\(2)SESSION_ID=0\(2)0\(1)\(3)case\(2)TIME_STAMP=1\(2)0\(1)\(3)case\(2)PAGE_NAME=2\(2)0\(1)\(3)case\(2)PORTLET_NAME=3\(2)0\(1)\(3)case\(2)ACTION=4\(2)0\(1)\(3)case\(2)ACCOUNT=5\(2)0\(1)\(3)case\(2)CUST_ID=6\(2)0\(1)\(3)case\(2)ID=7\(2)0\(1)\(3)case\(2)STAF_NUM=8\(2)0\(1)\(3)case\(2)CHANL_ID=9\(2)0\(1)\(3)case\(2)APP_NUM=10\(2)0\(1)\(3)case\(2)GLOBL_SESSN_ID=11\(2)0)
     */
    (lkSessionID, lkTimeStamp, lkPageName, lkPortletName, lkAction, lkAccount, lkCustId, lkID, lkStaffNum, lkChanlId, lkAppNum, lkGlblSessnId, lkJoinNewMIAs)
  }

}
