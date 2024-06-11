from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def TextInput_173(spark: SparkSession) -> DataFrame:
    
    colNames = ["Time", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
    data = [["08:00:00 AM", "484.00", "537.24", "515.75", "505.44", "429.62"],
            ["08:30:00 AM", "583.00", "647.13", "621.24", "608.82", "517.50"],
            ["09:00:00 AM", "650.00", "721.50", "692.64", "678.79", "576.97"],
            ["09:30:00 AM", "654.00", "725.94", "696.90", "682.96", "580.52"],
            ["10:00:00 AM", "674.00", "748.14", "718.21", "703.85", "598.27"],
            ["10:30:00 AM", "707.00", "784.77", "753.38", "738.31", "627.56"],
            ["11:00:00 AM", "663.00", "735.93", "706.49", "692.36", "588.51"],
            ["11:30:00 AM", "670.00", "743.70", "713.95", "699.67", "594.72"],
            ["12:00:00 PM", "569.00", "631.59", "606.33", "594.20", "505.07"],
            ["12:30:00 PM", "630.00", "699.30", "671.33", "657.90", "559.22"],
            ["13:00:00 PM", "637.00", "707.07", "678.79", "665.21", "565.43"],
            ["13:30:00 PM", "769.00", "853.59", "819.45", "803.06", "682.60"],
            ["14:00:00 PM", "677.00", "751.47", "721.41", "706.98", "600.94"],
            ["14:30:00 PM", "648.00", "719.28", "690.51", "676.70", "575.19"],
            ["15:00:00 PM", "556.00", "617.16", "592.47", "580.62", "493.53"],
            ["15:30:00 PM", "460.00", "510.60", "490.18", "480.37", "408.32"],
            ["16:00:00 PM", "332.00", "368.52", "353.78", "346.70", "294.70"],
            ["16:30:00 PM", "210.00", "233.10", "223.78", "219.30", "186.41"],
            ["17:00:00 PM", "108.00", "119.88", "115.08", "112.78", "95.87"],
            ["17:30:00 PM", "74.00", "82.14", "78.85", "77.28", "65.69"]]
    rows = [Row(*row) for row in data]
    schema = StructType([
            StructField("Time", StringType(), nullable = True),
         StructField("Monday", StringType(), nullable = True),
         StructField("Tuesday", StringType(), nullable = True),
         StructField("Wednesday", StringType(), nullable = True),
         StructField("Thursday", StringType(), nullable = True),
         StructField("Friday", StringType(), nullable = True)

    ])
    out0 = spark.createDataFrame(rows, schema)

    return out0
