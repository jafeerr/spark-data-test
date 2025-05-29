# spark-data-test

## Overview

`spark-data-test` provides utilities to compare two Spark DataFrames or datasets, generating detailed reports on matches, mismatches, and missing records. It is designed for data validation, ETL testing, and regression testing in Spark pipelines.

## Usage

### 1. Compare DataFrames Directly

Use `run_comparison_job_from_dfs` to compare two Spark DataFrames directly.

#### Function Signature

```python
run_comparison_job_from_dfs(
    spark: SparkSession,
    job_name: str,
    source_df: DataFrame,
    target_df: DataFrame,
    params: DatasetParams,
    output_config: OutputConfig
)
```

#### Parameters

- `spark`: The active `SparkSession`.
- `job_name`: Name for the comparison job (used in output paths).
- `source_df`: Source DataFrame.
- `target_df`: Target DataFrame.
- `params`: An instance of `DatasetParams` specifying dataset name, primary keys, columns to select/drop, etc.
- `output_config`: An instance of `OutputConfig` specifying output directory, file format, Spark write options, etc.

#### Example

```python
from spark_data_test.jobs.comparison_job import run_comparison_job_from_dfs
from spark_data_test.entities.config import DatasetParams, OutputConfig

params = DatasetParams(
    dataset_name="my_table",
    primary_keys=["id"]
)
output_config = OutputConfig(
    output_dir="/tmp/comparison_results"
)

run_comparison_job_from_dfs(spark, "my_job", df1, df2, params, output_config)
```

---

### 2. Compare Using Config (YAML/JSON/dict)

Use `run_comparison_job` to compare multiple datasets using a configuration dictionary or object.

#### Function Signature

```python
run_comparison_job(
    spark: SparkSession,
    config: dict or ComparisonJobConfig
)
```

#### Parameters

- `spark`: The active `SparkSession`.
- `config`: A dictionary or `ComparisonJobConfig` instance describing one or more datasets to compare, their source/target configs, and output config.

#### Example

```python
from spark_data_test.jobs.comparison_job import run_comparison_job

config = {
    "job_name": "multi_dataset_job",
    "dataset_configs": [
        {
            "params": {
                "dataset_name": "table1",
                "primary_keys": ["id"]
            },
            "source_config": {
                "path": "/data/source/table1",
                "file_format": "parquet"
            },
            "target_config": {
                "path": "/data/target/table1",
                "file_format": "parquet"
            }
        }
    ],
    "output_config": {
        "output_dir": "/tmp/comparison_results"
    }
}

run_comparison_job(spark, config)
```

---

## Output Files

After running a comparison job, the following files/directories are generated under the specified `output_dir` and `job_name`:

### **overall_test_report**

Summary DataFrame with row counts, matched counts, duplicate counts, missing rows, and test status for each dataset.

| dataset_name | count                | matched_count | duplicate_count         | missing_rows           | test_status |
|--------------|----------------------|---------------|------------------------|------------------------|-------------|
| table1       | {"source": 100, "target": 98} | 97            | {"source": 0, "target": 1} | {"source": 1, "target": 3} | PASSED      |

---

### **col_lvl_test_report**

Column-level report showing the count of unmatched values for each non-key column.

| dataset_name | column_name | unmatched_rows_count |
|--------------|-------------|---------------------|
| table1       | colA        | 2                   |
| table1       | colB        | 0                   |

---

### **row_lvl_test_report**

Row-level report with primary keys, duplicate count, missing row status, and match status for each row.

| dataset_name | id | duplicate_count | missing_row_status | all_rows_matched |
|--------------|----|----------------|--------------------|------------------|
| table1       | 1  | 0              | PRESENT_IN_BOTH    | true             |
| table1       | 2  | 0              | MISSING_AT_TARGET  | false            |

---

### **unmatched_rows/**

Directory containing one file per column with all rows where that column did not match between source and target.

Example for `unmatched_rows/colA`:

| dataset_name | id | colA_src | colA_target |
|--------------|----|----------|-------------|
| table1       | 5  | foo      | bar         |
| table1       | 8  | baz      | qux         |

All outputs are written in the format specified by `output_file_format` (default: parquet).

---

## Notes

- The package requires PySpark and is intended for use in Spark environments.
- For more details on configuration options, see the `entities/config.py` dataclasses.