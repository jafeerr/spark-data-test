import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
from spark_data_test.jobs.comparison_job import run_comparison_job_from_dfs, run_comparison_job
from spark_data_test.entities.config import DatasetParams, OutputConfig, TestParams, ComparisonJobConfig, DataframeConfig, DatasetConfig
from spark_data_test.constants.common_constants import *
from pyspark.testing import assertDataFrameEqual

from dataclasses import replace

job_name = "unit-testing"

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()

@pytest.fixture
def sample_data(spark):
    df1 = spark.createDataFrame([
    Row(id=1, value='foo', value2='foo', value3= True, value4 = 1.0),
    Row(id=3, value=None, value2=None, value3 = False, value4 = 3.0),
    Row(id=3, value=None, value2=None, value3 = False, value4 = 3.0),
    Row(id=4, value=None, value2=None, value3 = False, value4 = 4.0),
    Row(id=7, value='foo7', value2='foo2', value3 = False, value4 = 7.1),
    ])
    df2 = spark.createDataFrame([
     Row(id=3, value=None, value2=None, value3 = False, value4 = 3.0),
    Row(id=3, value=None, value2=None, value3 = False, value4 = 3.0),
    Row(id=7, value='foo', value2='foo3', value3 = False, value4 = 7.0),
    ])
    return df1, df2

@pytest.fixture
def configs(tmp_path):
    job_params = DatasetParams(
        dataset_name="pytest_job",
        primary_keys=["id"],
        test_params= TestParams(difference_tolerance=0.1)
    )
    output_config = OutputConfig(
        output_dir=str(tmp_path),
        output_file_format="parquet",
        no_of_partitions=1,
    )
    return job_params, output_config

@pytest.fixture
def get_comparison_job_from_dfs_results(spark, sample_data, configs):
    df1, df2 = sample_data
    job_params, output_config = configs

    run_comparison_job_from_dfs(spark, job_name,  df1, df2, job_params, output_config)
    result = {}
    result[OVERALL_TEST_REPORT_KEY] = spark.read.parquet(f"{output_config.output_dir}/{job_name}/{OVERALL_TEST_REPORT_KEY}")
    result[ROW_LVL_TEST_REPORT_KEY] = spark.read.parquet(f"{output_config.output_dir}/{job_name}/{ROW_LVL_TEST_REPORT_KEY}")
    result[COL_LVL_TEST_REPORT_KEY] = spark.read.parquet(f"{output_config.output_dir}/{job_name}/{COL_LVL_TEST_REPORT_KEY}")
    return result

@pytest.fixture
def get_comparison_job_results(spark,tmp_path, sample_data, configs):
    df1, df2 = sample_data
    job_params, output_config = configs
    output_config = replace(output_config,output_dir= f"{str(tmp_path)}/comparison_job")
    
    df1.write.mode("overwrite").parquet(f"{str(tmp_path)}/{job_name}/inputs/df1")
    df2.write.mode("overwrite").parquet(f"{str(tmp_path)}/{job_name}/inputs/df2")

    source_config = DataframeConfig(path = f"{str(tmp_path)}/{job_name}/inputs/df1")
    target_config = DataframeConfig(path = f"{str(tmp_path)}/{job_name}/inputs/df2")
    ds_config = DatasetConfig(params=job_params, source_config=source_config, target_config=target_config)
    comparison_job_config = ComparisonJobConfig(job_name = job_name, dataset_configs = [ds_config], output_config = output_config)

    run_comparison_job(spark, comparison_job_config)
    result = {}
    result[OVERALL_TEST_REPORT_KEY] = spark.read.parquet(f"{output_config.output_dir}/{job_name}/{OVERALL_TEST_REPORT_KEY}")
    result[ROW_LVL_TEST_REPORT_KEY] = spark.read.parquet(f"{output_config.output_dir}/{job_name}/{ROW_LVL_TEST_REPORT_KEY}")
    result[COL_LVL_TEST_REPORT_KEY] = spark.read.parquet(f"{output_config.output_dir}/{job_name}/{COL_LVL_TEST_REPORT_KEY}")
    return result

def test_run_comparison_job_results(spark, get_comparison_job_results, get_comparison_job_from_dfs_results):
    assertDataFrameEqual(get_comparison_job_results[OVERALL_TEST_REPORT_KEY], get_comparison_job_from_dfs_results[OVERALL_TEST_REPORT_KEY])
    assertDataFrameEqual(get_comparison_job_results[ROW_LVL_TEST_REPORT_KEY], get_comparison_job_from_dfs_results[ROW_LVL_TEST_REPORT_KEY])
    assertDataFrameEqual(get_comparison_job_results[COL_LVL_TEST_REPORT_KEY], get_comparison_job_from_dfs_results[COL_LVL_TEST_REPORT_KEY])


def test_overall_test_report(get_comparison_job_from_dfs_results):
    row = get_comparison_job_from_dfs_results.get(OVERALL_TEST_REPORT_KEY).first()

    assert row[COUNT_COL][SOURCE_COL] == 5 and row[COUNT_COL][TARGET_COL]== 3
    assert row[MATCHED_COUNT_COL] == 1
    assert row[DUPLICATE_COUNT_COL][SOURCE_COL]== 1 and row[DUPLICATE_COUNT_COL][TARGET_COL] == 1
    assert row[MISSING_ROWS_COL][SOURCE_COL] == 0 and row[MISSING_ROWS_COL][TARGET_COL] == 2
    assert row[TEST_STATUS_COL] == FAILED_STATUS

def test_row_level_test_report(get_comparison_job_from_dfs_results):
    row_level_report = get_comparison_job_from_dfs_results.get(ROW_LVL_TEST_REPORT_KEY).cache()
    assert row_level_report.count() == 4

    # Check for specific rows
    row1 = row_level_report.filter(row_level_report.id == 1).first()
    assert row1[MISSING_ROW_STATUS_COL] == MISSING_AT_TARGET_STATUS

    row2 = row_level_report.filter(row_level_report.id == 3).first()
    assert row2[MISSING_ROW_STATUS_COL] == PRESENT_IN_BOTH_STATUS and row2[DUPLICATE_COUNT_COL] == 3 and row2[ALL_ROWS_MATCHED_COL]

    row4 = row_level_report.filter(row_level_report.id == 7).first()
    assert not row4[ALL_ROWS_MATCHED_COL]
    

def test_column_level_test_report(get_comparison_job_from_dfs_results):
    col_level_report = get_comparison_job_from_dfs_results.get(COL_LVL_TEST_REPORT_KEY).cache()
    assert col_level_report.count() == 4

    # Check for specific rows
    value_col = col_level_report.filter(col(COL_NAME) == "value").first()
    assert value_col[UNMATCHED_ROWS_COUNT_COL] == 1

    value2_col = col_level_report.filter(col(COL_NAME) == "value2").first()
    assert value2_col[UNMATCHED_ROWS_COUNT_COL] == 1

    value3_col = col_level_report.filter(col(COL_NAME) == "value3").first()
    assert value3_col[UNMATCHED_ROWS_COUNT_COL] == 0

    value4_col = col_level_report.filter(col(COL_NAME) == "value4").first()
    assert value4_col[UNMATCHED_ROWS_COUNT_COL] == 0