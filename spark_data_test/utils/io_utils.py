from spark_data_test.constants.common_constants import (
    OVERWRITE_MODE,
    TABLE_FMT
)
def write_result(df, path, output_config):
    """
    Write the DataFrame to the specified path in the format defined in output_config.
    
    Args:
        df (DataFrame): The DataFrame to write.
        path (str): The path where the DataFrame should be written.
        output_config (OutputConfig): Configuration for output options.
    """

    if output_config.no_of_partitions > 0:
        df = df.coalesce(output_config.no_of_partitions) if output_config.no_of_partitions < df.rdd.getNumPartitions() else df.repartition(output_config.no_of_partitions)

    df.write.options(**output_config.spark_options).mode(OVERWRITE_MODE).format(
            output_config.output_file_format
        ).save(path)
    
def read_dataframe(spark, dataframe_config):
    """
    Read a DataFrame from the specified path and format.
    
    Args:
        spark (SparkSession): The Spark session to use for reading.
        dataframe_config (DataframeConfig): Configuration for the DataFrame to read.
        
    Returns:
        DataFrame: The read DataFrame.
    """
    if TABLE_FMT == dataframe_config.file_format:
        return spark.read.table(dataframe_config.path)
    else:
        return spark.read.options(**dataframe_config.spark_options).format(
            dataframe_config.file_format
        ).load(dataframe_config.path)
