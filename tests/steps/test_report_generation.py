import pytest
from pyspark.sql import SparkSession, DataFrame
from pytest_bdd import given, parsers, when, then, scenarios

from pyspark_bdd import report_generator
from tests.steps.step_utils import str_to_df, assert_spark_dataframe_equal

scenarios("../features/report_generation.feature")


@given(
    parsers.parse("the following transactions:\n{table}"),
    target_fixture="input_transactions",
)
def parse_transactions(spark: SparkSession, table: str) -> DataFrame:
    return str_to_df(spark, table)


@when("we generate the per-store loyalty report", target_fixture="result")
def generate_report(input_transactions: DataFrame) -> DataFrame:
    return report_generator.generate_report(input_transactions)


@then(parsers.parse("the report output should be:\n{table}"))
def verify_result(spark: SparkSession, table: str, result: DataFrame):
    expected_result = str_to_df(spark, table)
    comparison_schema = expected_result.schema.names
    assert_spark_dataframe_equal(
        result.select(comparison_schema), expected_result, *comparison_schema
    )


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    yield spark
    spark.stop()
