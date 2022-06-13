import re

from pandas.testing import assert_frame_equal
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import ParseException


def assert_spark_dataframe_equal(left: DataFrame, right: DataFrame, *sort_cols: str):
    if sort_cols:
        assert_frame_equal(
            left.sort(*sort_cols).toPandas(), right.sort(*sort_cols).toPandas()
        )
    else:
        assert_frame_equal(left.toPandas(), right.toPandas())


def str_to_df(spark: SparkSession, table_str: str) -> DataFrame:
    substitutions = [
        ("^ *", ""),  # Remove leading spaces
        (" *$", ""),  # Remove trailing spaces
        (r" *\| *", "|"),  # Remove spaces between columns
        (r"^\|", ""),  # Remove redundant leading delimiter
        (r"\|$", ""),  # Remove redundant trailing delimiter
    ]

    for pattern, replacement in substitutions:
        table_str = re.sub(pattern, replacement, table_str, flags=re.MULTILINE)

    table_lines = table_str.split("\n")

    schema_row, *data_rows = table_lines

    schema = schema_row.replace("|", ",")

    try:
        schema_struct = spark.createDataFrame(
            spark.sparkContext.emptyRDD(), schema=schema
        ).schema

    except ParseException as e:
        raise Exception(
            f"Unable to parse the schema provided in the scenario table: {schema}"
        ) from e

    return (
        spark.read.option("sep", "|")
        .option("nullValue", "null")
        .csv(path=spark.sparkContext.parallelize(data_rows), schema=schema_struct)
    )
