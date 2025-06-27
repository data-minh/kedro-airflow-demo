import time
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def load_and_sleep(input_df: DataFrame, sleep_duration_seconds: int = 1) -> DataFrame:
    """
    Loads data, pauses the Spark session for observation, and then returns the DataFrame.
    """
    
    time.sleep(sleep_duration_seconds) # Dừng thực thi tại đây

    # Bạn có thể thêm một số thao tác nhỏ sau đó để chứng minh DataFrame vẫn hoạt động
    return input_df

def transform_before_load_to_primary(input_df_1: DataFrame, input_df_2: DataFrame) -> DataFrame:
    """
    Join input_df_1 and input_df_2 on processed_date == primary_date,
    and return DataFrame with id, name, processed_date from input_df_1.
    """

    joined_df = input_df_1.join(
        input_df_2,
        input_df_1["processed_date"] == input_df_2["created_at"],
        how="inner"
    ).select(
        input_df_1["id"],
        input_df_1["name"],
        input_df_1["processed_date"]
    )
    
    return joined_df


def transform_before_load_to_feature(input_df_1: DataFrame, input_df_2: DataFrame, input_df_3: DataFrame) -> DataFrame:
    """
    Join input_df_1 and input_df_2 on processed_date == primary_date,
    and return DataFrame with id, name, processed_date from input_df_1.
    """

    joined_df = input_df_1.join(
        input_df_2,
        input_df_1["processed_date"] == input_df_2["primary_date"],
        how="inner"
    ).select(
        input_df_1["id"],
        input_df_1["name"],
        input_df_1["processed_date"]
    )

    final_df = joined_df.join(
        input_df_3,
        joined_df["processed_date"] == input_df_3["processed_date"],
        how="inner"
    ).select(
        joined_df["id"],
        joined_df["name"],
        joined_df["processed_date"]
    )
    
    return final_df