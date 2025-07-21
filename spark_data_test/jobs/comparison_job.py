import pyspark.sql.functions as f
from pyspark.sql.types import (
    ByteType, ShortType, IntegerType, LongType,
    FloatType, DoubleType, DecimalType, StructField, StringType, BooleanType
)
from pyspark.sql import Window
from spark_data_test.utils.spark_utils import  set_value_ifnull
from spark_data_test.utils.io_utils import read_dataframe, write_result
from spark_data_test.utils.config_reader import parse_comparison_job_config, dict_to_dataclass
from spark_data_test.entities.config import DatasetParams, OutputConfig, ComparisonJobConfig
from spark_data_test.constants.common_constants import *
from functools import reduce

numeric_types = [
    ByteType(), ShortType(), IntegerType(), LongType(),
    FloatType(), DoubleType(), DecimalType()
]

def __add_matched_column(df, struct_field, test_params):
    if struct_field.dataType in numeric_types:
        return df.withColumn(
            MATCHED_COL_SUFFIX.format(struct_field.name),
            f.abs(
                f.col(SRC_COL_SUFFIX.format(struct_field.name)) -
                f.col(TGT_COL_SUFFIX.format(struct_field.name))
            ) <= test_params.difference_tolerance
        )
    else:
        return df.withColumn(
            MATCHED_COL_SUFFIX.format(struct_field.name),
            f.col(SRC_COL_SUFFIX.format(struct_field.name)).eqNullSafe(
                f.col(TGT_COL_SUFFIX.format(struct_field.name))
            ),
        )

def __apply_source_target_transformations(df, suffix, non_key_cols, params):
    return (
        df.select(*params.select_cols)
        .drop(*params.drop_cols)
        .withColumn(
            suffix.format(CHK_SUM_COL),
            f.sha2(f.concat_ws(EMPTY_STR, *non_key_cols), 256),
        )
        .withColumnsRenamed(
            dict(map(lambda column_name: (column_name, suffix.format(column_name)), non_key_cols))
        )
    )

def __process_unmatched_records(unmatched_records, target_df_schema, key_cols, test_params):
    non_key_struct_fields = [field for field in target_df_schema.fields if field.name not in key_cols]
    unmatched_records = reduce(
        lambda df, struct_field: __add_matched_column(df, struct_field, test_params),
        non_key_struct_fields,
        unmatched_records
    ).drop(ALL_ROWS_MATCHED_COL)
    matched_columns = [
        f.col(column_name)
        for column_name in unmatched_records.columns
        if column_name.endswith(MATCHED_SUFFIX)
    ]
    return unmatched_records.withColumn(
        ALL_ROWS_MATCHED_AFTR_TOL_COL,
        reduce(lambda col1, col2: col1 & col2, matched_columns)
    )

def __get_overall_test_report(spark, dataset_name, key_cols, source_df, target_df, row_lvl_report, matched_records):
    matched_count = matched_records.count()
    rows = [
        dataset_name,
        {SOURCE_COL: source_df.count(), TARGET_COL: target_df.count()},
        matched_count
    ]
    missing_rows_count = row_lvl_report.agg(
        f.create_map(
            f.lit(SOURCE_COL),
            f.sum(f.when(f.col(MISSING_ROW_STATUS_COL) == MISSING_AT_SOURCE_STATUS, 1).otherwise(0)),
            f.lit(TARGET_COL),
            f.sum(f.when(f.col(MISSING_ROW_STATUS_COL) == MISSING_AT_TARGET_STATUS, 1).otherwise(0))
        ).alias(MISSING_ROWS_COL)
    ).withColumn(DATASET_NAME_COL, f.lit(dataset_name))
    rows.extend([
        {
            SOURCE_COL: source_df.groupBy(*key_cols).count().where(f.col(COUNT_COL) > 1).count(),
            TARGET_COL: target_df.groupBy(*key_cols).count().where(f.col(COUNT_COL) > 1).count()
        }
    ])
    overall_report = (
        spark.createDataFrame([tuple(rows)], [DATASET_NAME_COL, COUNT_COL, MATCHED_COUNT_COL, DUPLICATE_COUNT_COL])
        .join(missing_rows_count, DATASET_NAME_COL, LEFT_JOIN)
        .withColumn(
            TEST_STATUS_COL,
            f.when(
                (f.col(COUNT_COL).getItem(SOURCE_COL) == f.col(MATCHED_COUNT_COL)) &
                (f.col(COUNT_COL).getItem(TARGET_COL) == f.col(MATCHED_COUNT_COL)),
                f.lit(PASSED_STATUS)
            ).otherwise(f.lit(FAILED_STATUS))
        )
    )
    return overall_report

def __get_column_level_test_report(dataset_name, unmatched_records, non_key_cols):
    unmatch_count_conds = map(
        lambda column_name: f.sum(
            f.when(~f.col(MATCHED_COL_SUFFIX.format(column_name)), 1).otherwise(0)
        ).alias(column_name),
        non_key_cols
    )
    col_lvl_report = (
        unmatched_records.agg(*unmatch_count_conds)
        .unpivot([], non_key_cols, variableColumnName=COL_NAME, valueColumnName=UNMATCHED_ROWS_COUNT_COL)
        .withColumn(
            UNMATCHED_ROWS_COUNT_COL,
            set_value_ifnull(UNMATCHED_ROWS_COUNT_COL, f.lit(0).cast(IntegerType()))
        )
        .withColumn(DATASET_NAME_COL, f.lit(dataset_name))
        .cache()
    )
    return col_lvl_report.select(DATASET_NAME_COL, COL_NAME, UNMATCHED_ROWS_COUNT_COL)

def __get_unmatched_records(dataset_name, unmatched_records, col_lvl_report, key_cols):
    unmatched_records_map = {}
    unmatched_rows_cols = [
        x.column_name
        for x in col_lvl_report.where(f.col(UNMATCHED_ROWS_COUNT_COL) > 0).select(COL_NAME).collect()
    ]
    for column_name in unmatched_rows_cols:
        unmatched_records_map[f'{dataset_name}/{column_name}'] = (
            unmatched_records.where(~f.col(MATCHED_COL_SUFFIX.format(column_name)))
            .select(key_cols + [SRC_COL_SUFFIX.format(column_name), TGT_COL_SUFFIX.format(column_name)])
            .distinct()
        )
    return unmatched_records_map

def __get_row_level_test_report(dataset_name, joined_df, key_cols):
    result = (
        joined_df.select(
            *key_cols,
            SRC_COL_SUFFIX.format(CHK_SUM_COL),
            TGT_COL_SUFFIX.format(CHK_SUM_COL),
            ALL_ROWS_MATCHED_COL
        )
        .withColumn(
            DUPLICATE_COUNT_COL,
            f.count("*").over(Window.partitionBy(*key_cols).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)) - 1
        )
        .withColumn(
            MISSING_ROW_STATUS_COL,
            f.when(
                f.col(SRC_COL_SUFFIX.format(CHK_SUM_COL)).isNull(), MISSING_AT_SOURCE_STATUS
            ).when(
                f.col(TGT_COL_SUFFIX.format(CHK_SUM_COL)).isNull(), MISSING_AT_TARGET_STATUS
            ).otherwise(f.lit(PRESENT_IN_BOTH_STATUS).cast(StringType()))
        )
        .drop(
            SRC_COL_SUFFIX.format(CHK_SUM_COL),
            TGT_COL_SUFFIX.format(CHK_SUM_COL)
        )
        .distinct()
        .withColumn(DATASET_NAME_COL, f.lit(dataset_name))
    )
    return result.select(DATASET_NAME_COL, *key_cols, DUPLICATE_COUNT_COL, MISSING_ROW_STATUS_COL, ALL_ROWS_MATCHED_COL)

def __write_results(comparison_result, job_name, output_config):
    write_result(
        comparison_result[COL_LVL_TEST_REPORT_KEY].orderBy(DATASET_NAME_COL),
        f"{output_config.output_dir}/{job_name}/{COL_LVL_TEST_REPORT_DIR}",
        output_config
    )
    write_result(
        comparison_result[ROW_LVL_TEST_REPORT_KEY].orderBy(DATASET_NAME_COL),
        f"{output_config.output_dir}/{job_name}/{ROW_LVL_TEST_REPORT_DIR}",
        output_config
    )
    for col_name, df in comparison_result[UNMATCHED_RECORDS_KEY].items():
        write_result(
            df,
            f"{output_config.output_dir}/{job_name}/{UNMATCHED_ROWS_DIR.format(col_name=col_name)}",
            output_config
        )
    write_result(
        comparison_result[OVERALL_TEST_REPORT_KEY].orderBy(DATASET_NAME_COL),
        f"{output_config.output_dir}/{job_name}/{OVERALL_TEST_REPORT_DIR}",
        output_config
    )

def compare_dataframes(spark, source_df, target_df, params):
    non_key_cols = list(set(source_df.columns) - set(params.primary_keys))
    target_df_schema = target_df.select(*params.select_cols).drop(*params.drop_cols).schema

    # Apply transformations (select, drop columns), add checksum & append _src/_target for all columns
    source_df = __apply_source_target_transformations(
        source_df,
        SRC_COL_SUFFIX,
        non_key_cols,
        params
    ).cache()
    target_df = __apply_source_target_transformations(
        target_df,
        TGT_COL_SUFFIX,
        non_key_cols,
        params
    ).cache()

    # Join Source and Target DF and match _chechsum of Source and Target
    joined_df = source_df.join(
        target_df, params.primary_keys, FULL_OUTER_JOIN
    ).cache()
    joined_df = __add_matched_column(
        joined_df,
        StructField(CHK_SUM_COL, StringType(), True),
        params.test_params
    ).withColumnRenamed(
        MATCHED_COL_SUFFIX.format(CHK_SUM_COL), ALL_ROWS_MATCHED_COL
    ).cache()

    # For records with unmatched _checksum values, perform a column-by-column comparison. For numeric fields, apply a difference_tolerance to determine if values are acceptably close.
    unmatched_rows_aftr_tolerance = __process_unmatched_records(
        joined_df.filter(
            (~f.col(ALL_ROWS_MATCHED_COL))
            & (f.col(SRC_COL_SUFFIX.format(CHK_SUM_COL)).isNotNull())
            & (f.col(TGT_COL_SUFFIX.format(CHK_SUM_COL)).isNotNull())
        ),
        target_df_schema,
        params.primary_keys,
        params.test_params
    )

    # Update the all_rows_matched column in joined_df for records that are considered matched after applying the difference_tolerance to numeric fields.
    joined_df = joined_df.join(
        unmatched_rows_aftr_tolerance.filter(f.col(ALL_ROWS_MATCHED_AFTR_TOL_COL)).select(*(params.primary_keys+[ALL_ROWS_MATCHED_AFTR_TOL_COL])),
        params.primary_keys,
        LEFT_JOIN
    ).withColumn(ALL_ROWS_MATCHED_COL, f.col(ALL_ROWS_MATCHED_COL) | f.coalesce(f.col(ALL_ROWS_MATCHED_AFTR_TOL_COL), f.lit(False).cast(BooleanType()))).cache()

    unmatched_records = unmatched_rows_aftr_tolerance.filter(~f.col(ALL_ROWS_MATCHED_AFTR_TOL_COL))
    matched_records = joined_df.filter(f.col(ALL_ROWS_MATCHED_COL) == True).dropDuplicates(params.primary_keys)

    # Generate reports 
    col_lvl_report = __get_column_level_test_report(
        params.dataset_name, unmatched_records, non_key_cols
    )
    row_lvl_report = __get_row_level_test_report(
        params.dataset_name, joined_df, params.primary_keys
    ).cache()
    unmatched_records_map = __get_unmatched_records(
        params.dataset_name, unmatched_records, col_lvl_report, params.primary_keys
    )
    overall_report = __get_overall_test_report(
        spark, params.dataset_name, params.primary_keys, source_df, target_df, row_lvl_report, matched_records
    )
    return {
        OVERALL_TEST_REPORT_KEY: overall_report,
        ROW_LVL_TEST_REPORT_KEY: row_lvl_report,
        COL_LVL_TEST_REPORT_KEY: col_lvl_report,
        UNMATCHED_RECORDS_KEY: unmatched_records_map,
    }

def run_comparison_job_from_dfs(spark, job_name, source_df, target_df, params, output_config):
    if isinstance(params, dict):
        params = dict_to_dataclass(DatasetParams, params)
    if isinstance(output_config, dict):
        output_config = dict_to_dataclass(OutputConfig, output_config)
    comparison_result = compare_dataframes(spark, source_df, target_df, params)
    __write_results(comparison_result, job_name, output_config)

def run_comparison_job(spark, config):
    if isinstance(config, dict):
        config: ComparisonJobConfig = parse_comparison_job_config(config)
    consolidated_result = {}
    for dataset_config in config.dataset_configs:
        comparison_result = compare_dataframes(
            spark,
            read_dataframe(spark, dataset_config.source_config),
            read_dataframe(spark, dataset_config.target_config),
            dataset_config.params
        )
        if bool(consolidated_result):
            consolidated_result[OVERALL_TEST_REPORT_KEY] = consolidated_result[OVERALL_TEST_REPORT_KEY].union(
                comparison_result[OVERALL_TEST_REPORT_KEY]
            )
            consolidated_result[ROW_LVL_TEST_REPORT_KEY] = consolidated_result[ROW_LVL_TEST_REPORT_KEY].union(
                comparison_result[ROW_LVL_TEST_REPORT_KEY]
            )
            consolidated_result[COL_LVL_TEST_REPORT_KEY] = consolidated_result[COL_LVL_TEST_REPORT_KEY].union(
                comparison_result[COL_LVL_TEST_REPORT_KEY]
            )
            consolidated_result[UNMATCHED_RECORDS_KEY].update(
                comparison_result[UNMATCHED_RECORDS_KEY]
            )
        else:
            consolidated_result = comparison_result
    __write_results(consolidated_result, config.job_name, config.output_config)