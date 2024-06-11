from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def DSN_edw_prod_Query_S_2678(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_DSN_edw_prod_Query_S_2678}")\
        .option("user", f"{Config.username_DSN_edw_prod_Query_S_2678}")\
        .option("password", f"{Config.password_DSN_edw_prod_Query_S_2678}")\
        .option(
          "query",
          """Select MBR.MBR_INDV_BE_KEY, 
PROC.PROC_CD, 
left(CLM.CLM_SVC_STRT_DT_SK,7) as YEARMONTH,
Sum(CLM_LN.CLM_LN_UNIT_CT) As PROC_COUNT 
From PROD.CLM_F As CLM 
Inner Join PROD.MBR_D As MBR On CLM.MBR_SK = MBR.MBR_SK 
Inner Join PROD.CLM_LN_F As CLM_LN On CLM_LN.CLM_SK = CLM.CLM_SK 
Inner Join PROD.PROC_CD_D As PROC On CLM_LN.CLM_LN_PROC_CD_SK = PROC.PROC_CD_SK 
Inner Join PROD.PROD_D on PROD_D.PROD_SK = CLM.PROD_SK
Where PROC.PROC_CD Between '10000' And '99999' 
And CLM.CLM_SVC_STRT_DT_SK Between CHAR(((Current Date + 1 Days - Day(Current Date) Days) - 60 Months), ISO) 
And CHAR((LAST_DAY(Current Date - 13 Months)), ISO) 
And CLM.SRC_SYS_CD In ('FACETS') 
And CLM.CLM_STTUS_CD In ('A02') 
And CLM.CLM_FINL_DISP_CD = 'ACPTD' 
And CLM.CLM_TYP_CD = 'MED' 
And CLM.CLM_CAT_CD = 'STD' 
And CLM.CLM_SUBTYP_CD In ('PR') 
And CLM.CLM_HOST_IN = 'N' 
And CLM.CLM_PD_DT_SK <= CHAR((LAST_DAY(Current Date - 1 Months)), ISO) 
And (PROD_D.PROD_SH_NM in ('PCB', 'BCARE', 'BLUE-SELECT', 'BLUESELECT+', 'BLUE-ACCESS', 'PC', 'HP', 'BMADVH', 'BMADVP') or PROD_D.FNCL_LOB_CD in ('M016', 'M116', 'M419', 'K419', 'M519', 'K519', 'M019', 'K019', 'M119', 'K119', 'M424', 'K424', 'M524', 'K524', 'M024', 'K024', 'M124', 'K124')) 
Group By MBR.MBR_INDV_BE_KEY, PROC.PROC_CD, left(CLM.CLM_SVC_STRT_DT_SK,7)"""
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
