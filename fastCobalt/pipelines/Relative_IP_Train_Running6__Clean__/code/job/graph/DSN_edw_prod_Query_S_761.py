from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def DSN_edw_prod_Query_S_761(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_DSN_edw_prod_Query_S_761}")\
        .option("user", f"{Config.username_DSN_edw_prod_Query_S_761}")\
        .option("password", f"{Config.password_DSN_edw_prod_Query_S_761}")\
        .option(
          "query",
          """SELECT PROD.MBR_D.MBR_INDV_BE_KEY, LEFT(CLM_PD_DT_SK, 7) AS YEARMONTH,
                                PROD.DIAG_CD_D.DIAG_CD_SK as PRI_DIAG_CD_SK,
\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\tPROD.DIAG_CD_D.DIAG_CD,
\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\tPROD.DIAG_CD_D.DIAG_CD_DESC,
\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\tSUM(CLM_LN_TOT_PAYBL_AMT) as Diag_CD_Cost,
                                row_number() Over (Partition By PROD.MBR_D.MBR_INDV_BE_KEY, LEFT(CLM_PD_DT_SK, 7) Order By SUM(CLM_LN_TOT_PAYBL_AMT) DESC) row_no 
                FROM PROD.CLM_F
                                JOIN PROD.MBR_D ON PROD.MBR_D.MBR_SK = PROD.CLM_F.MBR_SK
                                JOIN PROD.DIAG_CD_D ON PROD.CLM_F.DIAG_CD_1_SK = PROD.DIAG_CD_D.DIAG_CD_SK
                WHERE PROD.CLM_F.SRC_SYS_CD IN ('FACETS') AND
                                CLM_FINL_DISP_CD = 'ACPTD' AND
                                PROD.CLM_F.CLM_STTUS_CD IN ('A02','A08','A09') AND
                                CLM_TYP_CD = 'MED' AND
                                CLM_CAT_CD = 'STD' --AND
                                --PROD.MBR_D.MBR_SK = '-2123739906' --'-2110003661' --'-1981535244'
                                AND
                                (PROD.CLM_F.CLM_PD_DT_SK >= char (current date - day(current date) days +1 day - 60 months, ISO))        
\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\tAND PROD.CLM_F.CLM_PD_DT_SK <= char (current date - day(current date) days +1 day - 13 months, ISO)            
                                GROUP BY PROD.MBR_D.MBR_INDV_BE_KEY, LEFT(CLM_PD_DT_SK, 7),
                                PROD.DIAG_CD_D.DIAG_CD_SK, PROD.DIAG_CD_D.DIAG_CD_DESC, PROD.DIAG_CD_D.DIAG_CD"""
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
