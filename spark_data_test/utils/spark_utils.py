from functools import reduce
import pyspark.sql.functions as f

def set_value_ifnull(column_name, value):
    return f.when(f.col(column_name).isNull(), value).otherwise(f.col(column_name))

