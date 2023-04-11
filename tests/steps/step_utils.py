import re
from pandas.testing import assert_frame_equal
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructField,
    StringType,
    StructType,
    ArrayType,
    MapType,
)
from pyspark.sql.utils import ParseException


def assert_spark_dataframe_equal(left: DataFrame, right: DataFrame, *sort_cols: str):
    if sort_cols:
        assert_frame_equal(
            left.sort(*sort_cols).toPandas(), right.sort(*sort_cols).toPandas()
        )
    else:
        assert_frame_equal(left.toPandas(), right.toPandas())



from pyspark.sql.utils import ParseException


def gherkin_table_str_to_df(spark: SparkSession, table_str: str) -> DataFrame:
    """
    Converts a Gherkin data table string into a Spark DataFrame.

    Args:
        spark: Spark Session
        table_str: Gherkin table string e.g.
            | name: str | age: int | job: str |
            | ringo     | 82       | drummer  |
            | paul      | 80       | singer   |

    Returns:
        a Spark DataFrame representing the data defined in the Gherkin data table string

    Raises:
        ValueError: if the schema cannot be parsed

    """
    substitutions = [
        ("^ *", ""),  # Remove leading spaces
        (" *$", ""),  # Remove trailing spaces
        (r" *\| *", "|"),  # Remove spaces between columns
        (r"^\|", ""),  # Remove redundant leading delimiter
        (r"\|$", ""),  # Remove redundant trailing delimiter
        (r"\\", ""),  # Remove redundant trailing delimiter
    ]

    cleansed_table_str = table_str

    for pattern, replacement in substitutions:
        cleansed_table_str = re.sub(
            pattern, replacement, cleansed_table_str, flags=re.MULTILINE
        )

    table_lines = cleansed_table_str.split("\n")

    schema_row, *data_rows = table_lines

    schema = schema_row.replace("|", ",").replace("[", "<").replace("]", ">")

    try:
        schema_struct = spark.createDataFrame(
            spark.sparkContext.emptyRDD(), schema=schema
        ).schema

    except ParseException as e:
        raise ValueError(
            f"Unable to parse the schema provided in the scenario table: {schema}"
        ) from e

    stringified_schema = [
        StructField(field.name, StringType()) for field in schema_struct
    ]

    parsed_df = (
        spark.read.option("sep", "|")
        .option("nullValue", "null")
        .csv(
            path=spark.sparkContext.parallelize(data_rows),
            schema=StructType(stringified_schema),
        )
        .select(*[cast_str_column_to_actual_type(field) for field in schema_struct])
    )

    return parsed_df


def cast_str_column_to_actual_type(field: StructField) -> Column:
    complex_types = StructType, ArrayType, MapType
    is_complex_type = isinstance(field.dataType, complex_types)

    if is_complex_type:
        return F.from_json(F.col(field.name), field.dataType).alias(field.name)
    else:
        return F.col(field.name).cast(field.dataType)
