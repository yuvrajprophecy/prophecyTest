from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_756_left_UnionLeftOuter(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          ((col("in0.MBR_INDV_BE_KEY") == col("in1.MBR_INDV_BE_KEY")) & (col("in0.YEARMONTH") == col("in1.YEARMONTH"))),
          "leftouter"
        )\
        .select(col("in0.M19_Running3").alias("M19_Running3"), col("in0.E50").alias("E50"), col("in0.C7_Running6").alias("C7_Running6"), col("in0.E50_Running6").alias("E50_Running6"), col("in0.Z18").alias("Z18"), col("in0.F33_Running3").alias("F33_Running3"), col("in0.S3_Running6").alias("S3_Running6"), col("in0.MEMBER_AGE2").alias("MEMBER_AGE2"), col("in0.E00_Running3").alias("E00_Running3"), col("in0.E55").alias("E55"), col("in0.IP_Running6").alias("IP_Running6"), col("in0.J1_Running6").alias("J1_Running6"), col("in0.E15").alias("E15"), col("in0.B2_Running6").alias("B2_Running6"), col("in0.E10_Running6").alias("E10_Running6"), col("in0.E36").alias("E36"), col("in0.E30").alias("E30"), col("in0.P8_Running6").alias("P8_Running6"), col("in0.H6_Running6").alias("H6_Running6"), col("in0.YEARMONTH").alias("YEARMONTH"), col("in0.Z30").alias("Z30"), col("in0.E30_Running3").alias("E30_Running3"), col("in0.W4_Running6").alias("W4_Running6"), col("in0.E13_Running6").alias("E13_Running6"), col("in0.V5_Running6").alias("V5_Running6"), col("in0.E63").alias("E63"), col("in0.E04").alias("E04"), col("in0.I8_Running6").alias("I8_Running6"), col("in0.E44").alias("E44"), col("in0.M12").alias("M12"), col("in0.E50_Running3").alias("E50_Running3"), col("in0.Z32_Running3").alias("Z32_Running3"), col("in0.E79_Running6").alias("E79_Running6"), col("in0.F32_Running3").alias("F32_Running3"), col("in0.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"), col("in0.W3_Running6").alias("W3_Running6"), col("in0.E74").alias("E74"), col("in0.M45").alias("M45"), col("in0.T1_Running6").alias("T1_Running6"), col("in0.T6_Running6").alias("T6_Running6"), col("in0.M49_Running3").alias("M49_Running3"), col("in0.E07_Running6").alias("E07_Running6"), col("in0.E25").alias("E25"), col("in0.Q4_Running6").alias("Q4_Running6"), col("in0.I3_Running6").alias("I3_Running6"), col("in0.Q3_Running6").alias("Q3_Running6"), col("in0.E70_Running6").alias("E70_Running6"), col("in0.W0_Running6").alias("W0_Running6"), col("in0.P7_Running6").alias("P7_Running6"), col("in0.M42_Running3").alias("M42_Running3"), col("in0.N6_Running6").alias("N6_Running6"), col("in0.A0_Running6").alias("A0_Running6"), col("in0.M9_Running6").alias("M9_Running6"), col("in0.E83_Running6").alias("E83_Running6"), col("in0.A5_Running6").alias("A5_Running6"), col("in0.E03").alias("E03"), col("in0.E36_Running3").alias("E36_Running3"), col("in0.Z29").alias("Z29"), col("in0.E66").alias("E66"), col("in0.E25_Running6").alias("E25_Running6"), col("in1.Secondary_Rx_Running3").alias("Secondary_Rx_Running3"), col("in0.E52").alias("E52"), col("in0.E58_Running3").alias("E58_Running3"), col("in0.E11_Running6").alias("E11_Running6"), col("in0.E77_Running6").alias("E77_Running6"), col("in0.F31_Running3").alias("F31_Running3"), col("in0.E83_Running3").alias("E83_Running3"), col("in0.C9_Running6").alias("C9_Running6"), col("in0.E71_Running6").alias("E71_Running6"), col("in0.A8_Running6").alias("A8_Running6"), col("in0.E73_Running3").alias("E73_Running3"), col("in0.E83").alias("E83"), col("in0.F8_Running6").alias("F8_Running6"), col("in0.MBR_SK").alias("MBR_SK"), col("in0.O03_Running3").alias("O03_Running3"), col("in0.U0_Running6").alias("U0_Running6"), col("in0.O31").alias("O31"), col("in1.OfLast6_Main_Rx_Costs").alias("OfLast6_Main_Rx_Costs"), col("in0.I1_Running6").alias("I1_Running6"), col("in0.O7_Running6").alias("O7_Running6"), col("in0.Z08").alias("Z08"), col("in0.O23_Running3").alias("O23_Running3"), col("in0.E77").alias("E77"), col("in0.E72").alias("E72"), col("in0.Z09_Running3").alias("Z09_Running3"), col("in0.R3_Running6").alias("R3_Running6"), col("in0.W9_Running6").alias("W9_Running6"), col("in0.O20").alias("O20"), col("in0.M2_Running6").alias("M2_Running6"), col("in0.S6_Running6").alias("S6_Running6"), col("in0.W6_Running6").alias("W6_Running6"), col("in0.S0_Running6").alias("S0_Running6"), col("in0.E86_Running6").alias("E86_Running6"), col("in0.E40").alias("E40"), col("in0.L6_Running6").alias("L6_Running6"), col("in0.E87_Running3").alias("E87_Running3"), col("in0.F30_Running3").alias("F30_Running3"), col("in0.E22_Running6").alias("E22_Running6"), col("in0.Right_YEARMONTH").alias("Right_YEARMONTH"), col("in0.Relative_IP_Month").alias("Relative_IP_Month"), col("in0.F39_Running3").alias("F39_Running3"), col("in0.Z21_Running3").alias("Z21_Running3"), col("in0.O07_Running3").alias("O07_Running3"), col("in0.K7_Running6").alias("K7_Running6"), col("in0.R4_Running6").alias("R4_Running6"), col("in0.E85_Running3").alias("E85_Running3"), col("in0.H5_Running6").alias("H5_Running6"), col("in0.Right_YEARMONTH").alias("Left_Right_YEARMONTH"), col("in0.X0_Running6").alias("X0_Running6"), col("in0.C4_Running6").alias("C4_Running6"), col("in0.M10_Running3").alias("M10_Running3"), col("in0.M17").alias("M17"), col("in0.M17_Running3").alias("M17_Running3"), col("in0.Z38").alias("Z38"), col("in0.M15_Running3").alias("M15_Running3"), col("in0.H7_Running6").alias("H7_Running6"), col("in0.Z14").alias("Z14"), col("in0.O26_Running3").alias("O26_Running3"), col("in0.R14").alias("R14"), col("in0.R5_Running6").alias("R5_Running6"), col("in0.G6_Running6").alias("G6_Running6"), col("in0.IP_Running3").alias("IP_Running3"), col("in0.N9_Running6").alias("N9_Running6"), col("in0.E20_Running6").alias("E20_Running6"), col("in0.R2_Running6").alias("R2_Running6"), col("in0.E10_Running3").alias("E10_Running3"), col("in0.E26").alias("E26"), col("in0.E30_Running6").alias("E30_Running6"), col("in0.E89_Running6").alias("E89_Running6"), col("in0.I4_Running6").alias("I4_Running6"), col("in0.Z14_Running3").alias("Z14_Running3"), col("in0.O26").alias("O26"), col("in0.O21_Running3").alias("O21_Running3"), col("in0.F34_Running3").alias("F34_Running3"), col("in0.E23_Running6").alias("E23_Running6"), col("in0.V6_Running6").alias("V6_Running6"), col("in0.M40").alias("M40"), col("in0.E65_Running6").alias("E65_Running6"), col("in0.E32_Running6").alias("E32_Running6"), col("in0.O29").alias("O29"), col("in0.Z03").alias("Z03"), col("in0.E41_Running3").alias("E41_Running3"), col("in0.A9_Running6").alias("A9_Running6"), col("in0.T0_Running6").alias("T0_Running6"), col("in0.E34_Running3").alias("E34_Running3"), col("in0.O32_Running3").alias("O32_Running3"), col("in0.F30").alias("F30"), col("in0.E31_Running6").alias("E31_Running6"), col("in0.V1_Running6").alias("V1_Running6"), col("in0.E85").alias("E85"), col("in0.O31_Running3").alias("O31_Running3"), col("in0.I5_Running6").alias("I5_Running6"), col("in0.N3_Running6").alias("N3_Running6"), col("in0.P1_Running6").alias("P1_Running6"), col("in0.Z05_Running3").alias("Z05_Running3"), col("in0.O02").alias("O02"), col("in0.E06_Running3").alias("E06_Running3"), col("in0.Z3A").alias("Z3A"), col("in0.E60").alias("E60"), col("in0.B0_Running6").alias("B0_Running6"), col("in0.D4_Running6").alias("D4_Running6"), col("in0.C1_Running6").alias("C1_Running6"), col("in0.E58_Running6").alias("E58_Running6"), col("in0.E16_Running6").alias("E16_Running6"), col("in0.O24_Running3").alias("O24_Running3"), col("in0.E09").alias("E09"), col("in0.E84_Running3").alias("E84_Running3"), col("in0.E04_Running6").alias("E04_Running6"), col("in0.Right_MBR_INDV_BE_KEY").alias("Left_Right_MBR_INDV_BE_KEY"), col("in0.I2_Running6").alias("I2_Running6"), col("in0.D0_Running6").alias("D0_Running6"), col("in0.Z13_Running3").alias("Z13_Running3"), col("in0.K0_Running6").alias("K0_Running6"), col("in0.I9_Running6").alias("I9_Running6"), col("in0.E05_Running6").alias("E05_Running6"), col("in0.A6_Running6").alias("A6_Running6"), col("in0.O33_Running3").alias("O33_Running3"), col("in0.Z02").alias("Z02"), col("in0.M13_Running3").alias("M13_Running3"), col("in0.Z29_Running3").alias("Z29_Running3"), col("in0.V4_Running6").alias("V4_Running6"), col("in0.Q0_Running6").alias("Q0_Running6"), col("in0.M12_Running3").alias("M12_Running3"), col("in0.E73").alias("E73"), col("in0.E61_Running3").alias("E61_Running3"), col("in0.E43_Running6").alias("E43_Running6"), col("in0.A3_Running6").alias("A3_Running6"), col("in0.E20").alias("E20"), col("in0.Z17").alias("Z17"), col("in0.Z31").alias("Z31"), col("in0.O28_Running3").alias("O28_Running3"), col("in0.X1_Running6").alias("X1_Running6"), col("in0.E54_Running3").alias("E54_Running3"), col("in0.O20_Running3").alias("O20_Running3"), col("in0.F34").alias("F34"), col("in0.Z15_Running3").alias("Z15_Running3"), col("in0.O6_Running6").alias("O6_Running6"), col("in0.Z19").alias("Z19"), col("in0.V8_Running6").alias("V8_Running6"), col("in0.M11").alias("M11"), col("in0.H2_Running6").alias("H2_Running6"), col("in0.E55_Running3").alias("E55_Running3"), col("in0.E28_Running3").alias("E28_Running3"), col("in0.E76_Running3").alias("E76_Running3"), col("in0.E59_Running6").alias("E59_Running6"), col("in0.G2_Running6").alias("G2_Running6"), col("in0.Q7_Running6").alias("Q7_Running6"), col("in0.E84").alias("E84"), col("in0.V3_Running6").alias("V3_Running6"), col("in0.Q2_Running6").alias("Q2_Running6"), col("in0.E11").alias("E11"), col("in0.Z34").alias("Z34"), col("in0.Z28_Running3").alias("Z28_Running3"), col("in0.D3_Running6").alias("D3_Running6"), col("in0.E84_Running6").alias("E84_Running6"), col("in0.V9_Running6").alias("V9_Running6"), col("in0.M40_Running3").alias("M40_Running3"), col("in0.IP_Running3_Max").alias("IP_Running3_Max"), col("in0.E26_Running6").alias("E26_Running6"), col("in0.E03_Running6").alias("E03_Running6"), col("in0.L3_Running6").alias("L3_Running6"), col("in0.UniqueLabs_Running3").alias("UniqueLabs_Running3"), col("in0.E00").alias("E00"), col("in0.E58").alias("E58"), col("in0.L9_Running6").alias("L9_Running6"), col("in0.V7_Running6").alias("V7_Running6"), col("in0.O01_Running3").alias("O01_Running3"), col("in0.E31").alias("E31"), col("in0.E22").alias("E22"), col("in0.E40_Running3").alias("E40_Running3"), col("in0.S8_Running6").alias("S8_Running6"), col("in0.E51").alias("E51"), col("in0.Z13").alias("Z13"), col("in0.O36_Running3").alias("O36_Running3"), col("in0.R15_Running3").alias("R15_Running3"), col("in0.E2_Running6").alias("E2_Running6"), col("in0.O29_Running3").alias("O29_Running3"), col("in0.MEMBER_AGE").alias("MEMBER_AGE"), col("in0.E42_Running6").alias("E42_Running6"), col("in1.Tertiary_RxCosts_Running3").alias("Tertiary_RxCosts_Running3"), col("in0.Z2_Running6").alias("Z2_Running6"), col("in0.E76_Running6").alias("E76_Running6"), col("in0.F2_Running6").alias("F2_Running6"), col("in0.F7_Running6").alias("F7_Running6"), col("in0.Z33_Running3").alias("Z33_Running3"), col("in0.C6_Running6").alias("C6_Running6"), col("in0.C0_Running6").alias("C0_Running6"), col("in0.B8_Running6").alias("B8_Running6"), col("in0.E88").alias("E88"), col("in0.R17").alias("R17"), col("in0.E1_Running6").alias("E1_Running6"), col("in0.T3_Running6").alias("T3_Running6"), col("in0.First_IP").alias("First_IP"), col("in0.E72_Running3").alias("E72_Running3"), col("in0.Z00_Running3").alias("Z00_Running3"), col("in0.D1_Running6").alias("D1_Running6"), col("in0.E74_Running6").alias("E74_Running6"), col("in0.R0_Running6").alias("R0_Running6"), col("in0.IP_Running6_Max").alias("IP_Running6_Max"), col("in0.M6_Running6").alias("M6_Running6"), col("in0.E41").alias("E41"), col("in0.T7_Running6").alias("T7_Running6"), col("in0.M46_Running3").alias("M46_Running3"), col("in0.E07").alias("E07"), col("in0.E71_Running3").alias("E71_Running3"), col("in0.X8_Running6").alias("X8_Running6"), col("in0.E34_Running6").alias("E34_Running6"), col("in0.E63_Running3").alias("E63_Running3"), col("in0.Z12_Running3").alias("Z12_Running3"), col("in0.E52_Running3").alias("E52_Running3"), col("in0.C3_Running6").alias("C3_Running6"), col("in0.E09_Running6").alias("E09_Running6"), col("in0.E01_Running6").alias("E01_Running6"), col("in0.P2_Running6").alias("P2_Running6"), col("in0.E64_Running3").alias("E64_Running3"), col("in0.Z23").alias("Z23"), col("in0.O34").alias("O34"), col("in0.Y9_Running6").alias("Y9_Running6"), col("in0.E55_Running6").alias("E55_Running6"), col("in0.E29_Running3").alias("E29_Running3"), col("in0.E05_Running3").alias("E05_Running3"), col("in0.O9_Running6").alias("O9_Running6"), col("in0.B7_Running6").alias("B7_Running6"), col("in0.L2_Running6").alias("L2_Running6"), col("in0.L4_Running6").alias("L4_Running6"), col("in0.Z0_Running6").alias("Z0_Running6"), col("in0.Z37").alias("Z37"), col("in0.E8_Running6").alias("E8_Running6"), col("in0.Right_MBR_INDV_BE_KEY").alias("Right_MBR_INDV_BE_KEY"), col("in0.R18").alias("R18"), col("in0.E15_Running6").alias("E15_Running6"), col("in0.Z3_Running6").alias("Z3_Running6"), col("in0.E59").alias("E59"), col("in0.O23").alias("O23"), col("in0.IP_PreviousMonth").alias("IP_PreviousMonth"), col("in0.E6_Running6").alias("E6_Running6"), col("in0.Z20_Running3").alias("Z20_Running3"), col("in0.E61").alias("E61"), col("in0.E35_Running6").alias("E35_Running6"), col("in0.E08").alias("E08"), col("in0.W1_Running6").alias("W1_Running6"), col("in0.O3_Running6").alias("O3_Running6"), col("in0.F31").alias("F31"), col("in0.O30").alias("O30"), col("in0.Z09").alias("Z09"), col("in0.G7_Running6").alias("G7_Running6"), col("in0.Z18_Running3").alias("Z18_Running3"), col("in0.E51_Running6").alias("E51_Running6"), col("in0.R10_Running3").alias("R10_Running3"), col("in0.K3_Running6").alias("K3_Running6"), col("in0.O4_Running6").alias("O4_Running6"), col("in0.R13").alias("R13"), col("in0.O00").alias("O00"), col("in0.N4_Running6").alias("N4_Running6"), col("in0.F32").alias("F32"), col("in0.Z20").alias("Z20"), col("in0.O8_Running6").alias("O8_Running6"), col("in0.E09_Running3").alias("E09_Running3"), col("in0.Z23_Running3").alias("Z23_Running3"), col("in0.E44_Running3").alias("E44_Running3"), col("in0.M8_Running6").alias("M8_Running6"), col("in0.E60_Running6").alias("E60_Running6"), col("in0.R19_Running3").alias("R19_Running3"), col("in0.W7_Running6").alias("W7_Running6"), col("in0.E29").alias("E29"), col("in0.B6_Running6").alias("B6_Running6"), col("in0.E89_Running3").alias("E89_Running3"), col("in0.E03_Running3").alias("E03_Running3"), col("in0.Z38_Running3").alias("Z38_Running3"), col("in0.I0_Running6").alias("I0_Running6"), col("in0.Z33").alias("Z33"), col("in0.E65_Running3").alias("E65_Running3"), col("in0.Z36_Running3").alias("Z36_Running3"), col("in0.E10").alias("E10"), col("in0.R1_Running6").alias("R1_Running6"), col("in0.S7_Running6").alias("S7_Running6"), col("in0.Z17_Running3").alias("Z17_Running3"), col("in0.S4_Running6").alias("S4_Running6"), col("in0.M43").alias("M43"), col("in0.E76").alias("E76"), col("in0.CountOfLabs_Running3").alias("CountOfLabs_Running3"), col("in0.B1_Running6").alias("B1_Running6"), col("in0.M14").alias("M14"), col("in0.E75_Running6").alias("E75_Running6"), col("in0.Z34_Running3").alias("Z34_Running3"), col("in0.K1_Running6").alias("K1_Running6"), col("in0.E13_Running3").alias("E13_Running3"), col("in0.P5_Running6").alias("P5_Running6"), col("in0.D2_Running6").alias("D2_Running6"), col("in0.O1_Running6").alias("O1_Running6"), col("in0.O33").alias("O33"), col("in0.E70").alias("E70"), col("in0.R6_Running6").alias("R6_Running6"), col("in0.M1A").alias("M1A"), col("in0.R19").alias("R19"), col("in0.Z04_Running3").alias("Z04_Running3"), col("in0.E21").alias("E21"), col("in0.Z5_Running6").alias("Z5_Running6"), col("in0.O0_Running6").alias("O0_Running6"), col("in0.C5_Running6").alias("C5_Running6"), col("in0.D5_Running6").alias("D5_Running6"), col("in0.G4_Running6").alias("G4_Running6"), col("in0.P9_Running6").alias("P9_Running6"), col("in0.R9_Running6").alias("R9_Running6"), col("in0.S5_Running6").alias("S5_Running6"), col("in1.Tertiary_Rx_Running3").alias("Tertiary_Rx_Running3"), col("in0.Q1_Running6").alias("Q1_Running6"), col("in0.Z02_Running3").alias("Z02_Running3"), col("in0.E66_Running6").alias("E66_Running6"), col("in1.Primary_Rx_Running3").alias("Primary_Rx_Running3"), col("in0.E68_Running3").alias("E68_Running3"), col("in0.E07_Running3").alias("E07_Running3"), col("in0.E59_Running3").alias("E59_Running3"), col("in0.Y6_Running6").alias("Y6_Running6"), col("in0.O22_Running3").alias("O22_Running3"), col("in0.E54").alias("E54"), col("in0.E15_Running3").alias("E15_Running3"), col("in0.Z03_Running3").alias("Z03_Running3"), col("in0.C8_Running6").alias("C8_Running6"), col("in0.Z9_Running6").alias("Z9_Running6"), col("in0.E67_Running3").alias("E67_Running3"), col("in0.E21_Running6").alias("E21_Running6"), col("in0.O01").alias("O01"), col("in0.S9_Running6").alias("S9_Running6"), col("in0.J9_Running6").alias("J9_Running6"), col("in0.M48").alias("M48"), col("in0.E46_Running3").alias("E46_Running3"), col("in0.E06").alias("E06"), col("in0.N1_Running6").alias("N1_Running6"), col("in0.E65").alias("E65"), col("in0.E46").alias("E46"), col("in0.R14_Running3").alias("R14_Running3"), col("in0.G0_Running6").alias("G0_Running6"), col("in0.M5_Running6").alias("M5_Running6"), col("in0.Q9_Running6").alias("Q9_Running6"), col("in0.E78_Running3").alias("E78_Running3"), col("in0.E5_Running6").alias("E5_Running6"), col("in0.I7_Running6").alias("I7_Running6"), col("in0.E23").alias("E23"), col("in0.IP_Month_Number").alias("IP_Month_Number"), col("in0.Z08_Running3").alias("Z08_Running3"), col("in0.H1_Running6").alias("H1_Running6"), col("in0.O22").alias("O22"), col("in0.E40_Running6").alias("E40_Running6"), col("in0.M11_Running3").alias("M11_Running3"), col("in0.E51_Running3").alias("E51_Running3"), col("in0.S2_Running6").alias("S2_Running6"), col("in0.P6_Running6").alias("P6_Running6"), col("in0.Z4_Running6").alias("Z4_Running6"), col("in0.E02_Running3").alias("E02_Running3"), col("in0.O02_Running3").alias("O02_Running3"), col("in0.M3_Running6").alias("M3_Running6"), col("in0.E74_Running3").alias("E74_Running3"), col("in0.M18").alias("M18"), col("in0.H3_Running6").alias("H3_Running6"), col("in0.G3_Running6").alias("G3_Running6"), col("in0.F9_Running6").alias("F9_Running6"), col("in0.V2_Running6").alias("V2_Running6"), col("in0.E35_Running3").alias("E35_Running3"), col("in0.E20_Running3").alias("E20_Running3"), col("in0.Y8_Running6").alias("Y8_Running6"), col("in0.C2_Running6").alias("C2_Running6"), col("in0.E01").alias("E01"), col("in0.H9_Running6").alias("H9_Running6"), col("in0.E32").alias("E32"), col("in0.E34").alias("E34"), col("in0.E87").alias("E87"), col("in0.K9_Running6").alias("K9_Running6"), col("in0.M14_Running3").alias("M14_Running3"), col("in0.O08").alias("O08"), col("in0.T5_Running6").alias("T5_Running6"), col("in0.M45_Running3").alias("M45_Running3"), col("in0.O30_Running3").alias("O30_Running3"), col("in0.Z22").alias("Z22"), col("in0.O25_Running3").alias("O25_Running3"), col("in0.Z1_Running6").alias("Z1_Running6"), col("in0.R18_Running3").alias("R18_Running3"), col("in0.E26_Running3").alias("E26_Running3"), col("in0.E24_Running6").alias("E24_Running6"), col("in0.K6_Running6").alias("K6_Running6"), col("in0.E22_Running3").alias("E22_Running3"), col("in0.E66_Running3").alias("E66_Running3"), col("in0.O34_Running3").alias("O34_Running3"), col("in0.E32_Running3").alias("E32_Running3"), col("in0.Concat_ORDER_TST_NM_Running3").alias("Concat_ORDER_TST_NM_Running3"), col("in0.M43_Running3").alias("M43_Running3"), col("in0.M42").alias("M42"), col("in0.E75_Running3").alias("E75_Running3"), col("in0.A7_Running6").alias("A7_Running6"), col("in0.Y7_Running6").alias("Y7_Running6"), col("in0.V0_Running6").alias("V0_Running6"), col("in0.J2_Running6").alias("J2_Running6"), col("in0.Z30_Running3").alias("Z30_Running3"), col("in0.D8_Running6").alias("D8_Running6"), col("in0.E53_Running6").alias("E53_Running6"), col("in0.Z05").alias("Z05"), col("in0.O35").alias("O35"), col("in0.Q8_Running6").alias("Q8_Running6"), col("in0.J4_Running6").alias("J4_Running6"), col("in0.E0_Running6").alias("E0_Running6"), col("in0.O2_Running6").alias("O2_Running6"), col("in0.K5_Running6").alias("K5_Running6"), col("in0.O35_Running3").alias("O35_Running3"), col("in0.E01_Running3").alias("E01_Running3"), col("in0.G8_Running6").alias("G8_Running6"), col("in0.Z22_Running3").alias("Z22_Running3"), col("in0.M10").alias("M10"), col("in0.E87_Running6").alias("E87_Running6"), col("in0.G5_Running6").alias("G5_Running6"), col("in0.MBR_GNDR_CD").alias("MBR_GNDR_CD"), col("in0.X9_Running6").alias("X9_Running6"), col("in0.Z16").alias("Z16"), col("in0.R16").alias("R16"), col("in0.R7_Running6").alias("R7_Running6"), col("in0.E53_Running3").alias("E53_Running3"), col("in0.O04").alias("O04"), col("in0.E11_Running3").alias("E11_Running3"), col("in0.N8_Running6").alias("N8_Running6"), col("in0.F33").alias("F33"), col("in0.A1_Running6").alias("A1_Running6"), col("in0.E28").alias("E28"), col("in0.E45_Running6").alias("E45_Running6"), col("in0.M47").alias("M47"), col("in0.O24").alias("O24"), col("in0.T4_Running6").alias("T4_Running6"), col("in0.F1_Running6").alias("F1_Running6"), col("in0.O09_Running3").alias("O09_Running3"), col("in0.E64_Running6").alias("E64_Running6"), col("in0.E79").alias("E79"), col("in0.H0_Running6").alias("H0_Running6"), col("in0.Z19_Running3").alias("Z19_Running3"), col("in0.HighestAlbumin_Running3").alias("HighestAlbumin_Running3"), col("in1.Primary_RxCosts_Running3").alias("Primary_RxCosts_Running3"), col("in0.J7_Running6").alias("J7_Running6"), col("in0.M7_Running6").alias("M7_Running6"), col("in0.S1_Running6").alias("S1_Running6"), col("in0.R12").alias("R12"), col("in0.E42").alias("E42"), col("in0.M16_Running3").alias("M16_Running3"), col("in0.E67_Running6").alias("E67_Running6"), col("in1.OfLast6_Main_Rx").alias("OfLast6_Main_Rx"), col("in0.P3_Running6").alias("P3_Running6"), col("in0.Z36").alias("Z36"), col("in0.Z12").alias("Z12"), col("in0.E68").alias("E68"), col("in0.A4_Running6").alias("A4_Running6"), col("in0.E27_Running6").alias("E27_Running6"), col("in0.M15").alias("M15"), col("in0.M1_Running6").alias("M1_Running6"), col("in0.A2_Running6").alias("A2_Running6"), col("in0.R11_Running3").alias("R11_Running3"), col("in0.E80").alias("E80"), col("in0.H4_Running6").alias("H4_Running6"), col("in0.Z7_Running6").alias("Z7_Running6"), col("in0.LowestGFR_Running3").alias("LowestGFR_Running3"), col("in0.E56_Running3").alias("E56_Running3"), col("in0.E35").alias("E35"), col("in0.E23_Running3").alias("E23_Running3"), col("in0.E85_Running6").alias("E85_Running6"), col("in0.H8_Running6").alias("H8_Running6"), col("in0.E08_Running6").alias("E08_Running6"), col("in0.E00_Running6").alias("E00_Running6"), col("in0.E53").alias("E53"), col("in0.O04_Running3").alias("O04_Running3"), col("in0.E21_Running3").alias("E21_Running3"), col("in0.B5_Running6").alias("B5_Running6"), col("in0.E24").alias("E24"), col("in0.O07").alias("O07"), col("in0.R12_Running3").alias("R12_Running3"), col("in0.Y3_Running6").alias("Y3_Running6"), col("in0.E46_Running6").alias("E46_Running6"), col("in0.E02_Running6").alias("E02_Running6"), col("in0.L5_Running6").alias("L5_Running6"), col("in0.M0_Running6").alias("M0_Running6"), col("in0.E7_Running6").alias("E7_Running6"), col("in0.Q5_Running6").alias("Q5_Running6"), col("in0.Y0_Running6").alias("Y0_Running6"), col("in0.E88_Running3").alias("E88_Running3"), col("in0.D6_Running6").alias("D6_Running6"), col("in0.E63_Running6").alias("E63_Running6"), col("in0.E54_Running6").alias("E54_Running6"), col("in0.L0_Running6").alias("L0_Running6"), col("in0.X5_Running6").alias("X5_Running6"), col("in0.O21").alias("O21"), col("in0.N7_Running6").alias("N7_Running6"), col("in0.Z37_Running3").alias("Z37_Running3"), col("in0.E08_Running3").alias("E08_Running3"), col("in0.Z32").alias("Z32"), col("in0.E05").alias("E05"), col("in0.E13").alias("E13"), col("in0.B4_Running6").alias("B4_Running6"), col("in0.E44_Running6").alias("E44_Running6"), col("in0.E45").alias("E45"), col("in0.X3_Running6").alias("X3_Running6"), col("in0.Y2_Running6").alias("Y2_Running6"), col("in0.F3_Running6").alias("F3_Running6"), col("in0.T2_Running6").alias("T2_Running6"), col("in0.L7_Running6").alias("L7_Running6"), col("in0.E16").alias("E16"), col("in0.D7_Running6").alias("D7_Running6"), col("in0.E75").alias("E75"), col("in0.E56").alias("E56"), col("in0.R10").alias("R10"), col("in0.TOT_MONTHS").alias("TOT_MONTHS"), col("in0.F5_Running6").alias("F5_Running6"), col("in0.O32").alias("O32"), col("in0.M48_Running3").alias("M48_Running3"), col("in0.E25_Running3").alias("E25_Running3"), col("in0.R13_Running3").alias("R13_Running3"), col("in0.E78").alias("E78"), col("in0.O09").alias("O09"), col("in0.X7_Running6").alias("X7_Running6"), col("in0.E41_Running6").alias("E41_Running6"), col("in1.Primary_Diagnosis").alias("Primary_Diagnosis"), col("in0.E61_Running6").alias("E61_Running6"), col("in0.R16_Running3").alias("R16_Running3"), col("in0.E64").alias("E64"), col("in0.E52_Running6").alias("E52_Running6"), col("in0.W2_Running6").alias("W2_Running6"), col("in0.M13").alias("M13"), col("in0.E29_Running6").alias("E29_Running6"), col("in0.F39").alias("F39"), col("in0.M46").alias("M46"), col("in0.F0_Running6").alias("F0_Running6"), col("in0.L8_Running6").alias("L8_Running6"), col("in0.Z16_Running3").alias("Z16_Running3"), col("in0.M4_Running6").alias("M4_Running6"), col("in0.Z8_Running6").alias("Z8_Running6"), col("in0.E77_Running3").alias("E77_Running3"), col("in0.Z6_Running6").alias("Z6_Running6"), col("in0.E71").alias("E71"), col("in0.N2_Running6").alias("N2_Running6"), col("in0.E86_Running3").alias("E86_Running3"), col("in0.RecordID").alias("RecordID"), col("in0.E70_Running3").alias("E70_Running3"), col("in0.Z31_Running3").alias("Z31_Running3"), col("in0.E28_Running6").alias("E28_Running6"), col("in0.M49").alias("M49"), col("in0.J3_Running6").alias("J3_Running6"), col("in0.Z28").alias("Z28"), col("in0.Z01").alias("Z01"), col("in0.E02").alias("E02"), col("in0.E42_Running3").alias("E42_Running3"), col("in0.F4_Running6").alias("F4_Running6"), col("in0.E27").alias("E27"), col("in0.M18_Running3").alias("M18_Running3"), col("in0.E67").alias("E67"), col("in0.E31_Running3").alias("E31_Running3"), col("in0.E80_Running6").alias("E80_Running6"), col("in0.E60_Running3").alias("E60_Running3"), col("in0.Z11").alias("Z11"), col("in0.J0_Running6").alias("J0_Running6"), col("in0.Z3A_Running3").alias("Z3A_Running3"), col("in0.W8_Running6").alias("W8_Running6"), col("in0.K4_Running6").alias("K4_Running6"), col("in0.R11").alias("R11"), col("in0.M19").alias("M19"), col("in0.W5_Running6").alias("W5_Running6"), col("in0.E3_Running6").alias("E3_Running6"), col("in0.E73_Running6").alias("E73_Running6"), col("in0.E4_Running6").alias("E4_Running6"), col("in0.E16_Running3").alias("E16_Running3"), col("in0.M1A_Running3").alias("M1A_Running3"), col("in0.E45_Running3").alias("E45_Running3"), col("in0.E72_Running6").alias("E72_Running6"), col("in0.M41_Running3").alias("M41_Running3"), col("in0.B3_Running6").alias("B3_Running6"), col("in0.M47_Running3").alias("M47_Running3"), col("in0.B9_Running6").alias("B9_Running6"), col("in0.E86").alias("E86"), col("in0.K2_Running6").alias("K2_Running6"), col("in0.Z39").alias("Z39"), col("in0.E06_Running6").alias("E06_Running6"), col("in0.F6_Running6").alias("F6_Running6"), col("in0.G1_Running6").alias("G1_Running6"), col("in0.R15").alias("R15"), col("in0.E04_Running3").alias("E04_Running3"), col("in0.E56_Running6").alias("E56_Running6"), col("in0.Z04").alias("Z04"), col("in0.J8_Running6").alias("J8_Running6"), col("in0.E43").alias("E43"), col("in0.O36").alias("O36"), col("in0.O00_Running3").alias("O00_Running3"), col("in0.E27_Running3").alias("E27_Running3"), col("in0.E78_Running6").alias("E78_Running6"), col("in0.Z21").alias("Z21"), col("in0.K8_Running6").alias("K8_Running6"), col("in0.O08_Running3").alias("O08_Running3"), col("in0.Z15").alias("Z15"), col("in0.J6_Running6").alias("J6_Running6"), col("in1.Secondary_RxCosts_Running3").alias("Secondary_RxCosts_Running3"), col("in0.O03").alias("O03"), col("in0.Z01_Running3").alias("Z01_Running3"), col("in0.Z11_Running3").alias("Z11_Running3"), col("in0.E88_Running6").alias("E88_Running6"), col("in0.E80_Running3").alias("E80_Running3"), col("in0.E79_Running3").alias("E79_Running3"), col("in0.R8_Running6").alias("R8_Running6"), col("in0.T8_Running6").alias("T8_Running6"), col("in0.L1_Running6").alias("L1_Running6"), col("in0.P0_Running6").alias("P0_Running6"), col("in0.O25").alias("O25"), col("in0.N0_Running6").alias("N0_Running6"), col("in0.M16").alias("M16"), col("in0.M41").alias("M41"), col("in0.E43_Running3").alias("E43_Running3"), col("in0.Z00").alias("Z00"), col("in0.IP_Record").alias("IP_Record"), col("in0.I6_Running6").alias("I6_Running6"), col("in0.E36_Running6").alias("E36_Running6"), col("in0.G9_Running6").alias("G9_Running6"), col("in0.O28").alias("O28"), col("in0.Z39_Running3").alias("Z39_Running3"), col("in0.N5_Running6").alias("N5_Running6"), col("in0.E24_Running3").alias("E24_Running3"), col("in0.E89").alias("E89"), col("in0.R17_Running3").alias("R17_Running3"), col("in0.Q6_Running6").alias("Q6_Running6"))