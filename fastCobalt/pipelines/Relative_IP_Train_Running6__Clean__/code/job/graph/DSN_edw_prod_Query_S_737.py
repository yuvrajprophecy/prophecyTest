from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def DSN_edw_prod_Query_S_737(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_DSN_edw_prod_Query_S_737}")\
        .option("user", f"{Config.username_DSN_edw_prod_Query_S_737}")\
        .option("password", f"{Config.password_DSN_edw_prod_Query_S_737}")\
        .option(
          "query",
          """SELECT PROD.MBR_D.MBR_INDV_BE_KEY, LEFT(PROD.CLM_F.CLM_PD_DT_SK, 7) as YEARMONTH,
                                PROD.NDC_D.NDC_SK as PRI_NDC_SK,
\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\tPROD.NDC_D.NDC_BRND_NM as PRI_NDC_BRND_NM,
\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\tSUM(CLM_LN_TOT_PAYBL_AMT) as NDC_Cost,
                                row_number() Over (Partition By PROD.MBR_D.MBR_INDV_BE_KEY, LEFT(PROD.CLM_F.CLM_PD_DT_SK, 7) Order By SUM(CLM_LN_TOT_PAYBL_AMT) DESC) row_no,
\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\tPROD.NDC_D.NDC  
                FROM PROD.CLM_F
                                JOIN PROD.MBR_D ON PROD.MBR_D.MBR_SK = PROD.CLM_F.MBR_SK
                                JOIN PROD.NDC_D ON PROD.CLM_F.NDC_SK = PROD.NDC_D.NDC_SK
                WHERE PROD.CLM_F.SRC_SYS_CD IN ('OPTUMRX','ESI','CVS','SAVRX') AND
                                CLM_FINL_DISP_CD = 'ACPTD' AND
                                PROD.CLM_F.CLM_STTUS_CD IN ('A02','A08','A09') AND
                                CLM_TYP_CD = 'MED' AND
                                CLM_CAT_CD = 'STD' --AND
                               -- PROD.MBR_D.MBR_SK = '-2123739906' --'-2110003661' --'-1981535244'
                                 AND
                                (PROD.CLM_F.CLM_PD_DT_SK >= char (current date - day(current date) days +1 day - 60 months, ISO) OR (PROD.CLM_F.SRC_SYS_CD = 'OPTUMRX' AND PROD.CLM_F.CLM_PD_DT_SK = '1753-01-01'))        
\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\tAND PROD.CLM_F.CLM_PD_DT_SK <= char (current date - day(current date) days +1 day - 13 months, ISO)                                                                                         
                GROUP BY PROD.MBR_D.MBR_INDV_BE_KEY, LEFT(PROD.CLM_F.CLM_PD_DT_SK, 7),
                                PROD.NDC_D.NDC_SK, PROD.NDC_D.NDC_BRND_NM, PROD.NDC_D.NDC """
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
