PARQUET_FMT = "parquet"
EMPTY_STR = ""
OVERWRITE_MODE = "overwrite"
TABLE_FMT = "table"

# join types
INNER_JOIN = "inner"
LEFT_JOIN = "left"
OUTER_JOIN = "outer"
FULL_OUTER_JOIN = "full_outer"
LEFT_JOIN = "left"

SRC_COL_SUFFIX = "{0}_src"
TGT_COL_SUFFIX = "{0}_target"
MATCHED_COL_SUFFIX = "{0}_matched"
MATCHED_SUFFIX = "_matched"

#common col names
CHK_SUM_COL = "_chk_sum"
COUNT_COL = "count"


#overall test report
DESCRIPTION_STR = "Description"
DATASET_NAME_COL = "dataset_name"
COUNT_COL = "count"
MATCHED_COUNT_COL = "matched_count"
MISSING_ROWS_COL = "missing_rows"
DUPLICATE_COUNT_COL = "duplicate_count"
SOURCE_COL = "source"
TARGET_COL = "target"
TEST_STATUS_COL = "test_status"
PASSED_STATUS = "PASSED"
FAILED_STATUS = "FAILED"

#column level report
COL_NAME = "column_name"
UNMATCHED_ROWS_COUNT_COL = "unmatched_rows_count"

#row level report
ALL_ROWS_MATCHED_COL = "all_rows_matched"
ALL_ROWS_MATCHED_AFTR_TOL_COL = "all_rows_matched_after_tolerance"
DUPLICATE_COUNT_COL = "duplicate_count"
MISSING_ROW_STATUS_COL = "missing_row_status"
MISSING_AT_SOURCE_STATUS = "MISSING_AT_SOURCE"
MISSING_AT_TARGET_STATUS = "MISSTING_AT_TARGET"
PRESENT_IN_BOTH_STATUS = "PRESENT_IN_BOTH"

#output report
OUTPUT_PATH_PREFIX = "{base_path}/{job_name}/{report_name}"
OVERALL_TEST_REPORT_DIR = "overall_test_report"
COL_LVL_TEST_REPORT_DIR = "col_lvl_test_report"
ROW_LVL_TEST_REPORT_DIR = "row_lvl_test_report"
UNMATCHED_ROWS_DIR = "unmatched_rows/{col_name}"
MISSING_ROWS_DIR = "missing_rows/{0}"

#result dict keys
OVERALL_TEST_REPORT_KEY = "overall_test_report"
COL_LVL_TEST_REPORT_KEY = "col_lvl_test_report"
ROW_LVL_TEST_REPORT_KEY = "row_lvl_test_report"
UNMATCHED_RECORDS_KEY = "unmatched_records"

#config keys
SRC_CONFIG_KEY = "source_config"
TGT_CONFIG_KEY = "target_config"
JOB_PARAMS_KEY = "job_params"
OUTPUT_CONFIG_KEY = "output_config"