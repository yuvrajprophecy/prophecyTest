import io.prophecy.libs._
import config.Context
import config._
import udfs.UDFs._
import udfs._
import graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_V_VOYAGE             = V_VOYAGE(context)
    val df_RTR_Insert_Update_SQ = RTR_Insert_Update_SQ(context)
    val (df_RTR_Insert_Update_out1, df_RTR_Insert_Update_out0) =
      RTR_Insert_Update(context,
                        df_RTR_Insert_Update_SQ,
                        df_RTR_Insert_Update_SQ,
                        df_RTR_Insert_Update_SQ,
                        df_RTR_Insert_Update_SQ
      )
    val df_UPD_DIM_VOYAGE_Update_SQ =
      UPD_DIM_VOYAGE_Update_SQ(context, df_RTR_Insert_Update_out1)
    val df_UPD_DIM_VOYAGE_Update =
      UPD_DIM_VOYAGE_Update(context, df_UPD_DIM_VOYAGE_Update_SQ)
    val df_VA_SALES_PRODUCT_OVERRIDES = VA_SALES_PRODUCT_OVERRIDES(context)
    val df_SQ_VOYAGE = SQ_VOYAGE(context,
                                 df_VA_SALES_PRODUCT_OVERRIDES,
                                 df_VA_SALES_PRODUCT_OVERRIDES,
                                 df_VA_SALES_PRODUCT_OVERRIDES
    )
    val df_LKP_V_GEOGRAPHIC_REGN_TRAVEL_AREA =
      LKP_V_GEOGRAPHIC_REGN_TRAVEL_AREA(context, df_SQ_VOYAGE)
    val df_EXP_Validate = EXP_Validate(
      context,
      df_LKP_V_GEOGRAPHIC_REGN_TRAVEL_AREA,
      df_LKP_V_GEOGRAPHIC_REGN_TRAVEL_AREA,
      df_LKP_V_GEOGRAPHIC_REGN_TRAVEL_AREA,
      df_LKP_V_GEOGRAPHIC_REGN_TRAVEL_AREA,
      df_LKP_V_GEOGRAPHIC_REGN_TRAVEL_AREA,
      df_LKP_V_GEOGRAPHIC_REGN_TRAVEL_AREA,
      df_LKP_V_GEOGRAPHIC_REGN_TRAVEL_AREA,
      df_LKP_V_GEOGRAPHIC_REGN_TRAVEL_AREA,
      df_LKP_V_GEOGRAPHIC_REGN_TRAVEL_AREA,
      df_LKP_V_GEOGRAPHIC_REGN_TRAVEL_AREA,
      df_LKP_V_GEOGRAPHIC_REGN_TRAVEL_AREA
    )
    val (df_RTR_Reject_Recs_out1, df_RTR_Reject_Recs_out0) =
      RTR_Reject_Recs(context, df_EXP_Validate)
    val df_EXP_Prepare_CRC_SQ_SQ =
      EXP_Prepare_CRC_SQ_SQ(context, df_RTR_Reject_Recs_out0)
    val df_VA_VOYAGE = VA_VOYAGE(context)
    val df_ERR_DIM_VOYAGE_Insert_SQ = ERR_DIM_VOYAGE_Insert_SQ(
      context,
      df_RTR_Reject_Recs_out1,
      df_RTR_Reject_Recs_out1
    )
    ERR_DIM_VOYAGE_Insert(context, df_ERR_DIM_VOYAGE_Insert_SQ)
    val df_EXP_Prepare_CRC_SQ =
      EXP_Prepare_CRC_SQ(context, df_EXP_Prepare_CRC_SQ_SQ)
    val df_LKP_V_ACTIVE_STATUS = LKP_V_ACTIVE_STATUS(context)
    val df_LKP_V_ITINERARY     = LKP_V_ITINERARY(context, df_SQ_VOYAGE)
    val df_EXP_Prepare_CRC     = EXP_Prepare_CRC(context, df_EXP_Prepare_CRC_SQ)
    val df_EP_Generate_CRC     = EP_Generate_CRC(context, df_EXP_Prepare_CRC)
    val df_EXP_Detect_Type_II =
      EXP_Detect_Type_II(context, df_EP_Generate_CRC, df_EP_Generate_CRC)
    val df_LKP_V_REGION_TRAVEL_AREA =
      LKP_V_REGION_TRAVEL_AREA(context, df_SQ_VOYAGE)
    val df_DIM_VOYAGE_Update_SQ =
      DIM_VOYAGE_Update_SQ(context, df_UPD_DIM_VOYAGE_Update)
    LKP_V_PRODUCT_CODE_SALES_PRODUCT_OVERRIDE_DESC_Lookup(context)
    val df_LKP_V_PORT         = LKP_V_PORT(context,         df_SQ_VOYAGE)
    val df_LKP_V_SHIP_PROFILE = LKP_V_SHIP_PROFILE(context, df_SQ_VOYAGE)
    val df_LKP_V_SALES_PRODUCT_OVERRIDES =
      LKP_V_SALES_PRODUCT_OVERRIDES(context, df_SQ_VOYAGE)
    LKP_V_SALES_PRODUCT_OVERRIDES_Lookup(context)
    val df_LKP_SAILING_TYPE      = LKP_SAILING_TYPE(context,      df_SQ_VOYAGE)
    val df_LKP_V_VOYAGE_DISPATCH = LKP_V_VOYAGE_DISPATCH(context, df_SQ_VOYAGE)
    val df_LKP_DIM_VOYAGE        = LKP_DIM_VOYAGE(context,        df_EXP_Prepare_CRC)
    val df_DIM_VOYAGE_Insert_SQ_SQ =
      DIM_VOYAGE_Insert_SQ_SQ(context, df_RTR_Insert_Update_out0)
    val df_DIM_VOYAGE_Insert_SQ = DIM_VOYAGE_Insert_SQ(
      context,
      df_DIM_VOYAGE_Insert_SQ_SQ,
      df_DIM_VOYAGE_Insert_SQ_SQ
    )
    DIM_VOYAGE_Insert(context, df_DIM_VOYAGE_Insert_SQ)
    LKP_V_REGION_TRAVEL_AREA_Lookup(context)
    DIM_VOYAGE_Update(context, df_DIM_VOYAGE_Update_SQ)
    LKP_V_ACTIVE_STATUS_Lookup(context)
    LKP_SAILING_TYPE_Lookup(context)
    LKP_DIM_VOYAGE_Lookup(context)
    LKP_V_VOYAGE_RESERVATION_STATUS_Lookup(context)
    LKP_V_VOYAGE_DISPATCH_Lookup(context)
    LKP_V_PORT_Lookup(context)
    val df_TempInput = TempInput(context)
    LKP_V_GEOGRAPHIC_REGN_TRAVEL_AREA_Lookup(context)
    val df_INPUT = INPUT(context, df_TempInput)
    val df_LKP_V_PRODUCT_CODE_SALES_PRODUCT_OVERRIDE_DESC =
      LKP_V_PRODUCT_CODE_SALES_PRODUCT_OVERRIDE_DESC(
        context,
        df_LKP_V_SALES_PRODUCT_OVERRIDES
      )
    val df_EXP_Get_NEXTVAL = EXP_Get_NEXTVAL(context, df_INPUT)
    LKP_V_ITINERARY_Lookup(context)
    val df_OUTPUT = OUTPUT(context, df_EXP_Get_NEXTVAL)
    val df_LKP_V_VOYAGE_RESERVATION_STATUS =
      LKP_V_VOYAGE_RESERVATION_STATUS(context, df_SQ_VOYAGE)
    LKP_V_SHIP_PROFILE_Lookup(context)
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.fromCLI(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
      .newSession()
    val context = Context(spark, config)
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/pppp")
    registerUDFs(spark)
    MetricsCollector.start(spark, "pipelines/pppp")
    apply(context)
    MetricsCollector.end(spark)
  }

}
