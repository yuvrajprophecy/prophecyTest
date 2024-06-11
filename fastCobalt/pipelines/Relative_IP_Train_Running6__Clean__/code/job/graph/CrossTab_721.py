from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def CrossTab_721(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("MBR_INDV_BE_KEY"), col("YEARMONTH"))
    df2 = df1.pivot(
        "First3",
        ["E50",  "Z18",  "E55",  "E15",  "E36",  "E30",  "Z30",  "E63",  "E04",  "E44",  "M12",  "E74",  "M45",  "E25",  "E03",  "Z29",  "E66",  "E52",          "E83",  "O31",  "Z08",  "E77",  "E72",  "O20",  "E40",  "M17",  "Z38",  "Z14",  "R14",  "E26",  "O26",  "M40",  "O29",  "Z03",  "F30",          "E85",  "O02",  "Z3A",  "E60",  "E09",  "Z02",  "E73",  "E20",  "Z17",  "Z31",  "F34",  "Z19",  "M11",  "E84",  "E11",  "Z34",  "E00",          "E58",  "E31",  "E22",  "E51",  "Z13",  "E88",  "R17",  "E41",  "E07",  "Z23",  "O34",  "Z37",  "R18",  "E59",  "O23",  "E61",  "E08",          "F31",  "O30",  "Z09",  "R13",  "O00",  "F32",  "Z20",  "E29",  "Z33",  "E10",  "M43",  "E76",  "M14",  "O33",  "E70",  "M1A",  "R19",          "E21",  "E54",  "O01",  "M48",  "E06",  "E65",  "E46",  "E23",  "O22",  "M18",  "E01",  "E32",  "E34",  "E87",  "O08",  "Z22",  "M42",          "Z05",  "O35",  "M10",  "Z16",  "R16",  "O04",  "F33",  "E28",  "M47",  "O24",  "E79",  "R12",  "E42",  "Z36",  "Z12",  "E68",  "M15",          "E80",  "E35",  "E53",  "E24",  "O07",  "O21",  "Z32",  "E05",  "E13",  "E45",  "E16",  "E75",  "E56",  "R10",  "O32",  "E78",  "O09",          "E64",  "M13",  "F39",  "M46",  "E71",  "M49",  "Z28",  "Z01",  "E02",  "E27",  "E67",  "Z11",  "R11",  "M19",  "E86",  "Z39",  "R15",          "Z04",  "E43",  "O36",  "Z21",  "Z15",  "O03",  "O25",  "M16",  "M41",  "Z00",  "O28",  "E89"]
    )

    return df2.agg(sum(col("Count")).alias("Count"))
