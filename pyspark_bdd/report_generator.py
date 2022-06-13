from pyspark.sql import DataFrame, functions as F, Column


def conditional_sum(column_to_sum: str, condition: Column) -> Column:
    return F.sum(F.when(condition, F.col(column_to_sum)).otherwise(0))


def generate_report(df: DataFrame) -> DataFrame:
    return df.groupby("store_name", "date").agg(
        conditional_sum("points_delta", df.transaction_type == "EARN").alias(
            "points_earned"
        ),
        conditional_sum("points_delta", df.transaction_type == "BURN").alias(
            "points_burned"
        ),
    )
