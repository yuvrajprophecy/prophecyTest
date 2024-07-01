from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_722(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("E05"), 
        col("E20"), 
        col("F30"), 
        col("E85"), 
        col("Z32"), 
        col("M15"), 
        col("R17"), 
        col("O09"), 
        col("Z03"), 
        col("YEARMONTH"), 
        col("M40"), 
        col("R15"), 
        col("M48"), 
        col("E40"), 
        col("E79"), 
        col("M11"), 
        col("E89"), 
        col("R10"), 
        col("E88"), 
        col("E06"), 
        col("Z08"), 
        col("M41"), 
        col("M19"), 
        col("E41"), 
        col("O32"), 
        col("E75"), 
        col("M49"), 
        col("Z09"), 
        col("M17"), 
        col("F39"), 
        col("E76"), 
        col("Z33"), 
        col("E58"), 
        col("E50"), 
        col("E08"), 
        col("M10"), 
        col("M42"), 
        col("E68"), 
        col("Z3A"), 
        col("M47"), 
        col("Z15"), 
        col("O24"), 
        col("E71"), 
        col("Z22"), 
        col("E04"), 
        col("E01"), 
        col("E10"), 
        col("E61"), 
        col("F32"), 
        col("E52"), 
        col("O30"), 
        col("Z36"), 
        col("Z30"), 
        col("E34"), 
        col("E70"), 
        col("R16"), 
        col("E86"), 
        col("R13"), 
        col("O03"), 
        col("Z12"), 
        col("E27"), 
        col("M13"), 
        col("R19"), 
        col("E15"), 
        col("Z16"), 
        col("Z04"), 
        col("E02"), 
        col("E03"), 
        col("M45"), 
        col("E60"), 
        col("O35"), 
        col("E28"), 
        col("Z31"), 
        col("Z11"), 
        col("Z23"), 
        col("E07"), 
        col("M14"), 
        col("M46"), 
        col("O07"), 
        col("E46"), 
        col("E78"), 
        col("E84"), 
        col("Z37"), 
        col("O22"), 
        col("Z17"), 
        col("E30"), 
        col("R11"), 
        col("E65"), 
        col("O31"), 
        col("E66"), 
        col("O02"), 
        col("R14"), 
        col("M43"), 
        col("O36"), 
        col("Z14"), 
        col("E31"), 
        col("E54"), 
        col("E45"), 
        col("MBR_INDV_BE_KEY"), 
        col("O25"), 
        col("O23"), 
        col("E32"), 
        col("F31"), 
        col("Z00"), 
        col("Z39"), 
        col("E00"), 
        col("E22"), 
        col("M16"), 
        col("E83"), 
        col("Z28"), 
        col("M18"), 
        col("Z02"), 
        col("M1A"), 
        col("E42"), 
        col("E72"), 
        col("E11"), 
        col("Z19"), 
        col("E67"), 
        col("E13"), 
        col("E16"), 
        col("Z05"), 
        col("E43"), 
        col("F33"), 
        col("E36"), 
        col("O34"), 
        col("O04"), 
        col("Z21"), 
        col("E23"), 
        col("E56"), 
        col("O00"), 
        col("Z01"), 
        col("O29"), 
        col("E29"), 
        col("Z29"), 
        col("O01"), 
        col("E64"), 
        col("E87"), 
        col("O26"), 
        col("O28"), 
        col("O08"), 
        col("R12"), 
        col("E24"), 
        col("E80"), 
        col("E25"), 
        col("Z18"), 
        col("E73"), 
        col("E63"), 
        col("E44"), 
        col("Z13"), 
        col("Z34"), 
        col("E35"), 
        col("E09"), 
        col("O33"), 
        col("E77"), 
        col("Z20"), 
        col("E55"), 
        col("Z38"), 
        col("E59"), 
        col("E51"), 
        col("O20"), 
        col("F34"), 
        col("R18"), 
        col("E74"), 
        col("E53"), 
        col("E21"), 
        col("O21"), 
        col("M12"), 
        col("E26")
    )