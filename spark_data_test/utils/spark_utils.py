from functools import reduce
import pyspark.sql.functions as f
from spark_data_test.constants.common_constants import (
    EMPTY_STR,
)
from pyspark.sql.types import StringType
def apply_spark_transformations(df, trans_function, columns:list=None):
    if columns is None:
        columns = df.columns  
    return reduce(lambda df, column_name: trans_function(df, column_name), columns, df)


def empty_str_to_null(column_name):
    return f.when(f.col(column_name) == EMPTY_STR, f.lit(None).cast(StringType())).otherwise(f.col(column_name).cast(StringType()))

def set_value_ifnull(column_name, value):
    return f.when(f.col(column_name).isNull(), value).otherwise(f.col(column_name))

