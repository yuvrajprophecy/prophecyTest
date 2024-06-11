from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def DSN_edw_prod_Query_s_543(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_DSN_edw_prod_Query_s_543}")\
        .option("user", f"{Config.username_DSN_edw_prod_Query_s_543}")\
        .option("password", f"{Config.password_DSN_edw_prod_Query_s_543}")\
        .option(
          "query",
          """select distinct MBR_D.MBR_INDV_BE_KEY,
\tCLM_LN_DIAG_I.DIAG_CD,
\tCLM_LN_DIAG_I.DIAG_CD_DESC,
\tCLM_LN_F.CLM_SK,
\tCLM_LN_F.CLM_LN_SK,
\tCLM_LN_F.CLM_ID,
\tCLM_LN_F.CLM_LN_SEQ_NO,
\tCLM_LN_F.CLM_LN_SVC_STRT_DT_SK,
\tCLM_LN_DIAG_I.CLM_LN_DIAG_SK,
\tCLM_LN_DIAG_I.DIAG_CD_SK,
\tCLM_LN_F.CLM_LN_DIAG_CD_1_SK,
\tCLM_LN_F.CLM_LN_DIAG_CD_2_SK,
\tCLM_LN_F.CLM_LN_DIAG_CD_3_SK,
\tCLM_LN_DIAG_I.DIAG_CD_TYP_CD,
\tSum(CLM_LN_F.CLM_LN_ALW_AMT) as CLM_LN_TOTL_ALWD 
from CLM_LN_DIAG_I 
\tinner join CLM_LN_F on CLM_LN_F.CLM_LN_SK = CLM_LN_DIAG_I.CLM_LN_SK 
\tinner join CLM_F on CLM_F.CLM_SK = CLM_LN_F.CLM_SK 
\tinner join MBR_D on MBR_D.MBR_SK = CLM_F.MBR_SK
where CLM_LN_F.CLM_LN_SVC_STRT_DT_SK Between char(((Current Date + 1 Days - Day(Current Date) Days) - 60 Months), ISO) 
\tand CHAR((LAST_Day(Current Date - 13 Months)), ISO) 
\tand CLM_LN_DIAG_I.DIAG_CD_TYP_CD = 'ICD10' 
\tand CLM_LN_DIAG_I.CLM_LN_DIAG_ORDNL_NM = 'PRIMARY' 
group by MBR_D.MBR_INDV_BE_KEY, CLM_LN_DIAG_I.DIAG_CD, CLM_LN_DIAG_I.DIAG_CD_DESC, CLM_LN_F.CLM_SK, CLM_LN_F.CLM_LN_SK, CLM_LN_F.CLM_ID, CLM_LN_F.CLM_LN_SEQ_NO, CLM_LN_F.CLM_LN_SVC_STRT_DT_SK, CLM_LN_DIAG_I.CLM_LN_DIAG_SK, CLM_LN_DIAG_I.DIAG_CD_SK, CLM_LN_F.CLM_LN_DIAG_CD_1_SK, CLM_LN_F.CLM_LN_DIAG_CD_2_SK, CLM_LN_F.CLM_LN_DIAG_CD_3_SK, CLM_LN_DIAG_I.DIAG_CD_TYP_CD"""
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
