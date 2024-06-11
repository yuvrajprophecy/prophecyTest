from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def CrossTab_546(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("MBR_INDV_BE_KEY"), col("YEARMONTH"))
    df2 = df1.pivot(
        "First2",
        ["D7",  "S9",  "B0",  "C6",  "A9",  "L8",  "R5",  "O2",  "Q0",  "H0",  "G1",  "S1",  "K8",  "X0",  "T7",  "R0",  "B5",  "H2",  "K7",  "E8",  "I5",  "S6",          "Z6",  "J6",  "V9",  "F9",  "A4",  "P3",  "L1",  "T2",  "Y7",  "O4",  "M9",  "Q3",  "N0",  "C1",  "W0",  "K0",  "D2",  "P5",  "F4",  "N3",          "G5",  "X1",  "M2",  "H1",  "E3",  "P2",  "Y2",  "H6",  "B9",  "W5",  "V4",  "N7",  "D8",  "S0",  "T8",  "K9",  "M6",  "L5",  "Q1",  "G0",          "R6",  "C8",  "C7",  "P0",  "S7",  "Q6",  "A5",  "J3",  "J2",  "I2",  "T1",  "O1",  "D1",  "Z2",  "B6",  "K4",  "Z3",  "P9",  "Q2",  "I1",          "J7",  "N6",  "I6",  "X5",  "H5",  "O7",  "C0",  "P8",  "Z7",  "R8",  "O8",  "O0",  "Q7",  "Y6",  "W4",  "W3",  "M5",  "L4",  "F3",  "E2",          "P1",  "L9",  "G4",  "V3",  "K3",  "F7",  "T5",  "E1",  "Q9",  "V2",  "C9",  "D5",  "E6",  "F2",  "M7",  "W8",  "N8",  "I8",  "J4",  "I0",          "V7",  "S3",  "B3",  "G8",  "H4",  "Z1",  "K5",  "A2",  "I3",  "C4",  "J1",  "S4",  "L6",  "X9",  "R3",  "H9",  "G3",  "Z4",  "M4",  "J8",          "A1",  "H8",  "R2",  "W7",  "P7",  "B7",  "D0",  "A6",  "O9",  "Y9",  "B2",  "I7",  "X8",  "T0",  "N5",  "O6",  "S8",  "Z8",  "G7",  "Q5",          "R7",  "Q8",  "V1",  "K2",  "E5",  "L3",  "V6",  "D4",  "T4",  "W2",  "F6",  "F1",  "C3",  "R9",  "N2",  "L7",  "O3",  "Z0",  "K6",  "Z5",          "I9",  "V8",  "Y0",  "M0",  "S5",  "E7",  "E0",  "J0",  "F8",  "A8",  "T6",  "R1",  "M8",  "R4",  "H3",  "B4",  "G2",  "L2",  "C5",  "N9",          "A3",  "D6",  "B1",  "I4",  "S2",  "X3",  "F0",  "G9",  "K1",  "H7",  "B8",  "U0",  "P6",  "G6",  "V5",  "Y8",  "W6",  "W1",  "A7",  "L0",          "V0",  "F5",  "A0",  "X7",  "C2",  "D3",  "Y3",  "Z9",  "N1",  "W9",  "Q4",  "M1",  "M3",  "J9",  "E4",  "N4",  "T3"]
    )

    return df2.agg(sum(col("Count")).alias("Count"))
