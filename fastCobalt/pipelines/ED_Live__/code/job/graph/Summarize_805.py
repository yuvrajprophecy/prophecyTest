from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_805(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("MBR_INDV_BE_KEY"))

    return df1.agg(
        percentile_approx(col("POLYPHARMACY_IN"), lit(0.5), lit(10000)).alias("Mode_POLYPHARMACY_IN"), 
        min(col("DistaneMiles")).alias("Min_DistaneMiles"), 
        max(col("mbr_age")).alias("Max_mbr_age"), 
        sum(col("`Household ED Visit Alw Amt`")).alias("Household ED Visit Alw Amt"), 
        percentile_approx(col("MED_HOME_ID"), lit(0.5), lit(10000)).alias("Mode_MED_HOME_ID"), 
        percentile_approx(col("DRUG_COUNT"), lit(0.5), lit(10000)).alias("Mode_DRUG_COUNT"), 
        percentile_approx(col("PRNT_GRP_SIC_NACIS_CD"), lit(0.5), lit(10000)).alias("Mode_PRNT_GRP_SIC_NACIS_CD"), 
        count_distinct(col("Week"), lit(0)).alias("CountDistinct_Week"), 
        percentile_approx(col("MBR_HOME_ADDR_ZIP_CD_5"), lit(0.5), lit(10000)).alias("Mode_MBR_HOME_ADDR_ZIP_CD_5"), 
        max(col("`History of ED Visits`")).alias("7-12 Month ED Visit History"), 
        last(col("`Members in Household`")).alias("Last_Members in Household"), 
        percentile_approx(col("PCP_FLAG"), lit(0.5), lit(10000)).alias("Mode_PCP_FLAG"), 
        percentile_approx(col("GRP_ID"), lit(0.5), lit(10000)).alias("Mode_GRP_ID"), 
        last(col("Month")).alias("Last_Month"), 
        percentile_approx(col("SUB_CNTGS_CNTY_CD"), lit(0.5), lit(10000)).alias("Mode_SUB_CNTGS_CNTY_CD"), 
        percentile_approx(col("PCMH_flag"), lit(0.5), lit(10000)).alias("Mode_PCMH_flag"), 
        percentile_approx(col("`Medical Home`"), lit(0.5), lit(10000)).alias("Mode_Medical Home"), 
        percentile_approx(col("PROD_CAT"), lit(0.5), lit(10000)).alias("Mode_PROD_CAT"), 
        percentile_approx(col("SUB_MKTNG_METRO_RURAL_CD"), lit(0.5), lit(10000)).alias("Mode_SUB_MKTNG_METRO_RURAL_CD"), 
        sum(col("drug")).alias("Sum_drug"), 
        sum(col("`Household ED Visit Tot Alw Amt`")).alias("Household ED Visit Tot Alw Amt"), 
        percentile_approx(col("MBR_RELSHP_NM"), lit(0.5), lit(10000)).alias("Mode_MBR_RELSHP_NM"), 
        percentile_approx(col("Race"), lit(0.5), lit(10000)).alias("Mode_Race"), 
        sum(col("injury")).alias("Sum_injury"), 
        max(col("CCI_Score")).alias("Max_CCI_Score"), 
        sum(col("`ED Visits from Members in Household (Not Member)`"))\
          .alias("Sum_ED Visits from Members in Household (Not Member)"), 
        sum(col("alcohol")).alias("Sum_alcohol"), 
        sum(col("`ED Visits`")).alias("ED Visits"), 
        max(col("`Non Emergent Likelihood`")).alias("Max_Non Emergent Likelihood"), 
        last(col("YMD")).alias("Last_YMD"), 
        percentile_approx(col("MBR_GNDR_CD"), lit(0.5), lit(10000)).alias("Mode_MBR_GNDR_CD"), 
        last(col("Week")).alias("Last_Week"), 
        max(col("`Non Emergent Binary`")).alias("Max_Non Emergent Binary"), 
        sum(col("`ED Visit Alw Amt`")).alias("ED Visit Alw Amt"), 
        percentile_approx(col("DRUG_CLASS_COUNT"), lit(0.5), lit(10000)).alias("Mode_DRUG_CLASS_COUNT"), 
        sum(col("psych")).alias("Sum_psych"), 
        percentile_approx(col("SPIRA_BNF_ID"), lit(0.5), lit(10000)).alias("Mode_SPIRA_BNF_ID"), 
        sum(col("`ED Visit Tot Alw Amt`")).alias("ED Visit Tot Alw Amt")
    )
