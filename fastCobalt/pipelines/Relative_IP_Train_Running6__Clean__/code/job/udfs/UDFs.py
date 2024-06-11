from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.lookups import (
    createLookup,
    createRangeLookup,
    lookup,
    lookup_last,
    lookup_match,
    lookup_count,
    lookup_row,
    lookup_row_reverse,
    lookup_nth
)


def alteryxOutputColumns(all_columns, columns_to_drop):
    conflicting_columns = [col.replace("in0_", "").replace("in1_", "") for col in all_columns]
    conflicting_columns = set([x for x in conflicting_columns if conflicting_columns.count(x) > 1])
    output_columns = []

    for col_name in all_columns:
        original_column_name = col_name.replace("in0_", "").replace("in1_", "")

        if col_name in columns_to_drop:
            continue
        else:
            if original_column_name in conflicting_columns and col_name.startswith("in1_"):
                output_columns.append(f"`in1_{original_column_name}` AS `Right_{original_column_name}`")
            elif col_name.startswith("in1_"):
                output_columns.append(f"`in1_{original_column_name}` AS `{original_column_name}`")
            elif col_name.startswith("in0_"):
                output_columns.append(f"`in0_{original_column_name}` AS `{original_column_name}`")
            else:
                output_columns.append(f"`{original_column_name}` AS `{original_column_name}`")

    return output_columns

def textToColumns(exploded_df, input_column, num_cols, rootName, field_separator):
    split_col_df = exploded_df.withColumn(
        "splitCol",
        call_spark_fcn("splitIntoMultipleColumnsUdf", col(input_column), lit(field_separator), lit(num_cols))\
          .alias("splitCol")
    )
    final_df = split_col_df
    index = - 1
    output_schema_columns = [f"{rootName}{i}" for i in range(1, num_cols + 1)]

    for column in output_schema_columns:
        index += 1
        final_df = final_df.withColumn(column, col("splitCol")[index])

    final_df = final_df.drop("splitCol").drop(input_column)

    return final_df

def transposeDataFrame(df, key_columns, data_columns):
    unpivoted_cols = (
        [col(column) for column in key_columns]
        + [lit(column).alias("Name") for column in data_columns]
        + [col(column).alias("Value") for column in data_columns]
    )

    return df.select(*unpivoted_cols)

def registerUDFs(spark: SparkSession):
    spark.udf.register("generateRowsUdf2704", generateRowsUdf2704)
    spark.udf.register("generateRowsUdf2652", generateRowsUdf2652)
    spark.udf.register("generateRowsUdf2666", generateRowsUdf2666)
    spark.udf.register("generateRowsUdf2758", generateRowsUdf2758)
    spark.udf.register("generateRowsUdf2622", generateRowsUdf2622)
    spark.udf.register("generateRowsUdf613", generateRowsUdf613)
    spark.udf.register("generateRowsUdf732", generateRowsUdf732)
    spark.udf.register("generateRowsUdf3750", generateRowsUdf3750)
    

    try:
        from prophecy.utils import ScalaUtil
        ScalaUtil.initializeUDFs(spark)
    except :
        pass

def generateRowsUdf2704Generator():
    

    def alteryxOutputColumns(all_columns, columns_to_drop):
        conflicting_columns = [col.replace("in0_", "").replace("in1_", "") for col in all_columns]
        conflicting_columns = set([x for x in conflicting_columns if conflicting_columns.count(x) > 1])
        output_columns = []

        for col_name in all_columns:
            original_column_name = col_name.replace("in0_", "").replace("in1_", "")

            if col_name in columns_to_drop:
                continue
            else:
                if original_column_name in conflicting_columns and col_name.startswith("in1_"):
                    output_columns.append(f"`in1_{original_column_name}` AS `Right_{original_column_name}`")
                elif col_name.startswith("in1_"):
                    output_columns.append(f"`in1_{original_column_name}` AS `{original_column_name}`")
                elif col_name.startswith("in0_"):
                    output_columns.append(f"`in0_{original_column_name}` AS `{original_column_name}`")
                else:
                    output_columns.append(f"`{original_column_name}` AS `{original_column_name}`")

        return output_columns

    def textToColumns(exploded_df, input_column, num_cols, rootName, field_separator):
        split_col_df = exploded_df.withColumn(
            "splitCol",
            call_spark_fcn("splitIntoMultipleColumnsUdf", col(input_column), lit(field_separator), lit(num_cols))\
              .alias("splitCol")
        )
        final_df = split_col_df
        index = - 1
        output_schema_columns = [f"{rootName}{i}" for i in range(1, num_cols + 1)]

        for column in output_schema_columns:
            index += 1
            final_df = final_df.withColumn(column, col("splitCol")[index])

        final_df = final_df.drop("splitCol").drop(input_column)

        return final_df

    def transposeDataFrame(df, key_columns, data_columns):
        unpivoted_cols = (
            [col(column) for column in key_columns]
            + [lit(column).alias("Name") for column in data_columns]
            + [col(column).alias("Value") for column in data_columns]
        )

        return df.select(*unpivoted_cols)

    @udf(returnType = ArrayType(IntegerType()))
    def func(NumRows: int, RecordNum: int):
        result = []
        previous = RecordNum

        while previous > (RecordNum - NumRows):
            result.append(previous)
            previous = int(previous) - 1

        return result

    return func

generateRowsUdf2704 = generateRowsUdf2704Generator()

def generateRowsUdf2652Generator():
    

    def alteryxOutputColumns(all_columns, columns_to_drop):
        conflicting_columns = [col.replace("in0_", "").replace("in1_", "") for col in all_columns]
        conflicting_columns = set([x for x in conflicting_columns if conflicting_columns.count(x) > 1])
        output_columns = []

        for col_name in all_columns:
            original_column_name = col_name.replace("in0_", "").replace("in1_", "")

            if col_name in columns_to_drop:
                continue
            else:
                if original_column_name in conflicting_columns and col_name.startswith("in1_"):
                    output_columns.append(f"`in1_{original_column_name}` AS `Right_{original_column_name}`")
                elif col_name.startswith("in1_"):
                    output_columns.append(f"`in1_{original_column_name}` AS `{original_column_name}`")
                elif col_name.startswith("in0_"):
                    output_columns.append(f"`in0_{original_column_name}` AS `{original_column_name}`")
                else:
                    output_columns.append(f"`{original_column_name}` AS `{original_column_name}`")

        return output_columns

    def textToColumns(exploded_df, input_column, num_cols, rootName, field_separator):
        split_col_df = exploded_df.withColumn(
            "splitCol",
            call_spark_fcn("splitIntoMultipleColumnsUdf", col(input_column), lit(field_separator), lit(num_cols))\
              .alias("splitCol")
        )
        final_df = split_col_df
        index = - 1
        output_schema_columns = [f"{rootName}{i}" for i in range(1, num_cols + 1)]

        for column in output_schema_columns:
            index += 1
            final_df = final_df.withColumn(column, col("splitCol")[index])

        final_df = final_df.drop("splitCol").drop(input_column)

        return final_df

    def transposeDataFrame(df, key_columns, data_columns):
        unpivoted_cols = (
            [col(column) for column in key_columns]
            + [lit(column).alias("Name") for column in data_columns]
            + [col(column).alias("Value") for column in data_columns]
        )

        return df.select(*unpivoted_cols)

    @udf(returnType = ArrayType(IntegerType()))
    def func(NumRows: int, RecordNum: int):
        result = []
        previous = RecordNum

        while previous > (RecordNum - NumRows):
            result.append(previous)
            previous = int(previous) - 1

        return result

    return func

generateRowsUdf2652 = generateRowsUdf2652Generator()

def generateRowsUdf2666Generator():
    

    def alteryxOutputColumns(all_columns, columns_to_drop):
        conflicting_columns = [col.replace("in0_", "").replace("in1_", "") for col in all_columns]
        conflicting_columns = set([x for x in conflicting_columns if conflicting_columns.count(x) > 1])
        output_columns = []

        for col_name in all_columns:
            original_column_name = col_name.replace("in0_", "").replace("in1_", "")

            if col_name in columns_to_drop:
                continue
            else:
                if original_column_name in conflicting_columns and col_name.startswith("in1_"):
                    output_columns.append(f"`in1_{original_column_name}` AS `Right_{original_column_name}`")
                elif col_name.startswith("in1_"):
                    output_columns.append(f"`in1_{original_column_name}` AS `{original_column_name}`")
                elif col_name.startswith("in0_"):
                    output_columns.append(f"`in0_{original_column_name}` AS `{original_column_name}`")
                else:
                    output_columns.append(f"`{original_column_name}` AS `{original_column_name}`")

        return output_columns

    def textToColumns(exploded_df, input_column, num_cols, rootName, field_separator):
        split_col_df = exploded_df.withColumn(
            "splitCol",
            call_spark_fcn("splitIntoMultipleColumnsUdf", col(input_column), lit(field_separator), lit(num_cols))\
              .alias("splitCol")
        )
        final_df = split_col_df
        index = - 1
        output_schema_columns = [f"{rootName}{i}" for i in range(1, num_cols + 1)]

        for column in output_schema_columns:
            index += 1
            final_df = final_df.withColumn(column, col("splitCol")[index])

        final_df = final_df.drop("splitCol").drop(input_column)

        return final_df

    def transposeDataFrame(df, key_columns, data_columns):
        unpivoted_cols = (
            [col(column) for column in key_columns]
            + [lit(column).alias("Name") for column in data_columns]
            + [col(column).alias("Value") for column in data_columns]
        )

        return df.select(*unpivoted_cols)

    @udf(returnType = ArrayType(IntegerType()))
    def func(NumRows: int, RecordNum: int):
        result = []
        previous = RecordNum

        while previous > (RecordNum - NumRows):
            result.append(previous)
            previous = int(previous) - 1

        return result

    return func

generateRowsUdf2666 = generateRowsUdf2666Generator()

def generateRowsUdf2758Generator():
    

    def alteryxOutputColumns(all_columns, columns_to_drop):
        conflicting_columns = [col.replace("in0_", "").replace("in1_", "") for col in all_columns]
        conflicting_columns = set([x for x in conflicting_columns if conflicting_columns.count(x) > 1])
        output_columns = []

        for col_name in all_columns:
            original_column_name = col_name.replace("in0_", "").replace("in1_", "")

            if col_name in columns_to_drop:
                continue
            else:
                if original_column_name in conflicting_columns and col_name.startswith("in1_"):
                    output_columns.append(f"`in1_{original_column_name}` AS `Right_{original_column_name}`")
                elif col_name.startswith("in1_"):
                    output_columns.append(f"`in1_{original_column_name}` AS `{original_column_name}`")
                elif col_name.startswith("in0_"):
                    output_columns.append(f"`in0_{original_column_name}` AS `{original_column_name}`")
                else:
                    output_columns.append(f"`{original_column_name}` AS `{original_column_name}`")

        return output_columns

    def textToColumns(exploded_df, input_column, num_cols, rootName, field_separator):
        split_col_df = exploded_df.withColumn(
            "splitCol",
            call_spark_fcn("splitIntoMultipleColumnsUdf", col(input_column), lit(field_separator), lit(num_cols))\
              .alias("splitCol")
        )
        final_df = split_col_df
        index = - 1
        output_schema_columns = [f"{rootName}{i}" for i in range(1, num_cols + 1)]

        for column in output_schema_columns:
            index += 1
            final_df = final_df.withColumn(column, col("splitCol")[index])

        final_df = final_df.drop("splitCol").drop(input_column)

        return final_df

    def transposeDataFrame(df, key_columns, data_columns):
        unpivoted_cols = (
            [col(column) for column in key_columns]
            + [lit(column).alias("Name") for column in data_columns]
            + [col(column).alias("Value") for column in data_columns]
        )

        return df.select(*unpivoted_cols)

    @udf(returnType = ArrayType(IntegerType()))
    def func(NumRows: int, RecordNum: int):
        result = []
        previous = RecordNum

        while previous > (RecordNum - NumRows):
            result.append(previous)
            previous = int(previous) - 1

        return result

    return func

generateRowsUdf2758 = generateRowsUdf2758Generator()

def generateRowsUdf2622Generator():
    

    def alteryxOutputColumns(all_columns, columns_to_drop):
        conflicting_columns = [col.replace("in0_", "").replace("in1_", "") for col in all_columns]
        conflicting_columns = set([x for x in conflicting_columns if conflicting_columns.count(x) > 1])
        output_columns = []

        for col_name in all_columns:
            original_column_name = col_name.replace("in0_", "").replace("in1_", "")

            if col_name in columns_to_drop:
                continue
            else:
                if original_column_name in conflicting_columns and col_name.startswith("in1_"):
                    output_columns.append(f"`in1_{original_column_name}` AS `Right_{original_column_name}`")
                elif col_name.startswith("in1_"):
                    output_columns.append(f"`in1_{original_column_name}` AS `{original_column_name}`")
                elif col_name.startswith("in0_"):
                    output_columns.append(f"`in0_{original_column_name}` AS `{original_column_name}`")
                else:
                    output_columns.append(f"`{original_column_name}` AS `{original_column_name}`")

        return output_columns

    def textToColumns(exploded_df, input_column, num_cols, rootName, field_separator):
        split_col_df = exploded_df.withColumn(
            "splitCol",
            call_spark_fcn("splitIntoMultipleColumnsUdf", col(input_column), lit(field_separator), lit(num_cols))\
              .alias("splitCol")
        )
        final_df = split_col_df
        index = - 1
        output_schema_columns = [f"{rootName}{i}" for i in range(1, num_cols + 1)]

        for column in output_schema_columns:
            index += 1
            final_df = final_df.withColumn(column, col("splitCol")[index])

        final_df = final_df.drop("splitCol").drop(input_column)

        return final_df

    def transposeDataFrame(df, key_columns, data_columns):
        unpivoted_cols = (
            [col(column) for column in key_columns]
            + [lit(column).alias("Name") for column in data_columns]
            + [col(column).alias("Value") for column in data_columns]
        )

        return df.select(*unpivoted_cols)

    @udf(returnType = ArrayType(IntegerType()))
    def func(NumRows: int, RecordNum: int):
        result = []
        previous = RecordNum

        while previous > (RecordNum - NumRows):
            result.append(previous)
            previous = int(previous) - 1

        return result

    return func

generateRowsUdf2622 = generateRowsUdf2622Generator()

def generateRowsUdf613Generator():
    

    def alteryxOutputColumns(all_columns, columns_to_drop):
        conflicting_columns = [col.replace("in0_", "").replace("in1_", "") for col in all_columns]
        conflicting_columns = set([x for x in conflicting_columns if conflicting_columns.count(x) > 1])
        output_columns = []

        for col_name in all_columns:
            original_column_name = col_name.replace("in0_", "").replace("in1_", "")

            if col_name in columns_to_drop:
                continue
            else:
                if original_column_name in conflicting_columns and col_name.startswith("in1_"):
                    output_columns.append(f"`in1_{original_column_name}` AS `Right_{original_column_name}`")
                elif col_name.startswith("in1_"):
                    output_columns.append(f"`in1_{original_column_name}` AS `{original_column_name}`")
                elif col_name.startswith("in0_"):
                    output_columns.append(f"`in0_{original_column_name}` AS `{original_column_name}`")
                else:
                    output_columns.append(f"`{original_column_name}` AS `{original_column_name}`")

        return output_columns

    def textToColumns(exploded_df, input_column, num_cols, rootName, field_separator):
        split_col_df = exploded_df.withColumn(
            "splitCol",
            call_spark_fcn("splitIntoMultipleColumnsUdf", col(input_column), lit(field_separator), lit(num_cols))\
              .alias("splitCol")
        )
        final_df = split_col_df
        index = - 1
        output_schema_columns = [f"{rootName}{i}" for i in range(1, num_cols + 1)]

        for column in output_schema_columns:
            index += 1
            final_df = final_df.withColumn(column, col("splitCol")[index])

        final_df = final_df.drop("splitCol").drop(input_column)

        return final_df

    def transposeDataFrame(df, key_columns, data_columns):
        unpivoted_cols = (
            [col(column) for column in key_columns]
            + [lit(column).alias("Name") for column in data_columns]
            + [col(column).alias("Value") for column in data_columns]
        )

        return df.select(*unpivoted_cols)

    @udf(returnType = ArrayType(IntegerType()))
    def func(NumRows: int, RecordNum: int):
        result = []
        previous = RecordNum

        while previous > (RecordNum - NumRows):
            result.append(previous)
            previous = int(previous) - 1

        return result

    return func

generateRowsUdf613 = generateRowsUdf613Generator()

def generateRowsUdf732Generator():
    

    def alteryxOutputColumns(all_columns, columns_to_drop):
        conflicting_columns = [col.replace("in0_", "").replace("in1_", "") for col in all_columns]
        conflicting_columns = set([x for x in conflicting_columns if conflicting_columns.count(x) > 1])
        output_columns = []

        for col_name in all_columns:
            original_column_name = col_name.replace("in0_", "").replace("in1_", "")

            if col_name in columns_to_drop:
                continue
            else:
                if original_column_name in conflicting_columns and col_name.startswith("in1_"):
                    output_columns.append(f"`in1_{original_column_name}` AS `Right_{original_column_name}`")
                elif col_name.startswith("in1_"):
                    output_columns.append(f"`in1_{original_column_name}` AS `{original_column_name}`")
                elif col_name.startswith("in0_"):
                    output_columns.append(f"`in0_{original_column_name}` AS `{original_column_name}`")
                else:
                    output_columns.append(f"`{original_column_name}` AS `{original_column_name}`")

        return output_columns

    def textToColumns(exploded_df, input_column, num_cols, rootName, field_separator):
        split_col_df = exploded_df.withColumn(
            "splitCol",
            call_spark_fcn("splitIntoMultipleColumnsUdf", col(input_column), lit(field_separator), lit(num_cols))\
              .alias("splitCol")
        )
        final_df = split_col_df
        index = - 1
        output_schema_columns = [f"{rootName}{i}" for i in range(1, num_cols + 1)]

        for column in output_schema_columns:
            index += 1
            final_df = final_df.withColumn(column, col("splitCol")[index])

        final_df = final_df.drop("splitCol").drop(input_column)

        return final_df

    def transposeDataFrame(df, key_columns, data_columns):
        unpivoted_cols = (
            [col(column) for column in key_columns]
            + [lit(column).alias("Name") for column in data_columns]
            + [col(column).alias("Value") for column in data_columns]
        )

        return df.select(*unpivoted_cols)

    @udf(returnType = ArrayType(IntegerType()))
    def func(NumRows: int, RecordNum: int):
        result = []
        previous = RecordNum

        while previous > (RecordNum - NumRows):
            result.append(previous)
            previous = int(previous) - 1

        return result

    return func

generateRowsUdf732 = generateRowsUdf732Generator()

def generateRowsUdf3750Generator():
    

    def alteryxOutputColumns(all_columns, columns_to_drop):
        conflicting_columns = [col.replace("in0_", "").replace("in1_", "") for col in all_columns]
        conflicting_columns = set([x for x in conflicting_columns if conflicting_columns.count(x) > 1])
        output_columns = []

        for col_name in all_columns:
            original_column_name = col_name.replace("in0_", "").replace("in1_", "")

            if col_name in columns_to_drop:
                continue
            else:
                if original_column_name in conflicting_columns and col_name.startswith("in1_"):
                    output_columns.append(f"`in1_{original_column_name}` AS `Right_{original_column_name}`")
                elif col_name.startswith("in1_"):
                    output_columns.append(f"`in1_{original_column_name}` AS `{original_column_name}`")
                elif col_name.startswith("in0_"):
                    output_columns.append(f"`in0_{original_column_name}` AS `{original_column_name}`")
                else:
                    output_columns.append(f"`{original_column_name}` AS `{original_column_name}`")

        return output_columns

    def textToColumns(exploded_df, input_column, num_cols, rootName, field_separator):
        split_col_df = exploded_df.withColumn(
            "splitCol",
            call_spark_fcn("splitIntoMultipleColumnsUdf", col(input_column), lit(field_separator), lit(num_cols))\
              .alias("splitCol")
        )
        final_df = split_col_df
        index = - 1
        output_schema_columns = [f"{rootName}{i}" for i in range(1, num_cols + 1)]

        for column in output_schema_columns:
            index += 1
            final_df = final_df.withColumn(column, col("splitCol")[index])

        final_df = final_df.drop("splitCol").drop(input_column)

        return final_df

    def transposeDataFrame(df, key_columns, data_columns):
        unpivoted_cols = (
            [col(column) for column in key_columns]
            + [lit(column).alias("Name") for column in data_columns]
            + [col(column).alias("Value") for column in data_columns]
        )

        return df.select(*unpivoted_cols)

    @udf(returnType = ArrayType(IntegerType()))
    def func(NumRows: int, RecordNum: int):
        result = []
        previous = RecordNum

        while previous > (RecordNum - NumRows):
            result.append(previous)
            previous = int(previous) - 1

        return result

    return func

generateRowsUdf3750 = generateRowsUdf3750Generator()
