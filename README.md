# Spark Data Test
[![Coverage Status](https://coveralls.io/repos/github/jafeerr/spark-data-test/badge.svg?branch=main)](https://coveralls.io/github/jafeerr/spark-data-test?branch=main)
[![License](https://img.shields.io/pypi/l/spark-data-test.svg)](https://pypi.python.org/pypi/spark-data-test/)
[![Version](https://img.shields.io/pypi/v/spark-data-test.svg)](https://pypi.python.org/pypi/spark-data-test/)
## Overview

`spark-data-test` provides utilities to compare two Spark DataFrames or datasets, generating detailed reports on matches, mismatches, and missing records. It is designed for data validation, ETL testing, and regression testing in Spark pipelines.

## Installation

To install, simply use `pip`:

```
$ pip install spark-data-test
```

## Requirements

Minimum Python version supported by `spark-data-test` is 3.7.

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
    params: DatasetParams|dict,
    output_config: OutputConfig|=dict
)
```

#### Parameters

- `spark`: The active `SparkSession`.
- `job_name`: Name for the comparison job (used in output paths).
- `source_df`: Source DataFrame.
- `target_df`: Target DataFrame.
- `params`: An instance of `DatasetParams` specifying dataset name, primary keys, columns to select/drop, etc.
- `output_config`: An instance of [`OutputConfig`](#outputconfig) specifying output directory, file format, Spark write options, etc.

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

### 2. Compare Using Config (dict/dataclasses)

Use `run_comparison_job` to compare multiple datasets using a configuration dictionary or object.

#### Function Signature

```python
run_comparison_job(
    spark: SparkSession,
    config: ComparisonJobConfig | dict
)
```

#### Parameters

- `spark`: The active `SparkSession`.
- `config`: A dictionary or [`ComparisonJobConfig`](#comparisonjobconfig) instance describing one or more datasets to compare, their source/target configs, and output config.

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

## Example Configuration (Python dict)

Below is an example of how to create a configuration dictionary for `run_comparison_job` using the dataclass structure:

```python
config = {
    "job_name": "sample_comparison_job",
    "dataset_configs": [
        {
            "params": {
                "dataset_name": "table1",
                "primary_keys": ["id"],
                "test_params": {"difference_tolerance": 0.1},
                "select_cols": ["id", "name", "value"],
                "drop_cols": []
            },
            "source_config": {
                "path": "/data/source/table1",
                "file_format": "parquet",
                "spark_options": {}
            },
            "target_config": {
                "path": "/data/target/table1",
                "file_format": "parquet",
                "spark_options": {}
            }
        },
        {
            "params": {
                "dataset_name": "table2",
                "primary_keys": ["key"],
                "test_params": {"difference_tolerance": 0.0},
                "select_cols": ["key", "amount"],
                "drop_cols": ["extra_col"]
            },
            "source_config": {
                "path": "/data/source/table2",
                "file_format": "csv",
                "spark_options": {"header": "true"}
            },
            "target_config": {
                "path": "/data/target/table2",
                "file_format": "csv",
                "spark_options": {"header": "true"}
            }
        }
    ],
    "output_config": {
        "output_dir": "/tmp/comparison_results",
        "output_file_format": "parquet",
        "spark_options": {},
        "no_of_partitions": -1
    }
}
```

You can pass this config directly to `run_comparison_job(spark, config)`.

---

## Configuration Dataclasses

Below are the main dataclasses used for configuration in `spark-data-test`. You can use these directly in Python or as a reference for your JSON configs.

### DatasetParams
Defines parameters for a single dataset comparison.
```python
@dataclass
class TestParams:
    difference_margin: float = 0.0  # Allowed numeric difference for matching numeric columns.
```
```python
from dataclasses import dataclass, field

@dataclass
class DatasetParams:
    dataset_name: str  # Name of the dataset/table
    primary_keys: list # List of primary key column names
    test_params: TestParams # Testing parameters for dataset (Optional)
    select_cols: list # Columns to select (default: all) (Optional)
    drop_cols: list # Columns to drop (default: none) (Optional)
```

### DataframeConfig

Defines how to read a DataFrame from storage.

```python
from dataclasses import dataclass, field

@dataclass
class DataframeConfig:
    path: str        # Path to the data (e.g., file or table)
    file_format: str # File format (parquet, csv, etc.) (default:parquet) (Optional)
    spark_options: dict # Spark read options (e.g., {"header": "true"}) (Optional)
```

### OutputConfig

Defines output options for writing comparison results.

```python
from dataclasses import dataclass, field

@dataclass
class OutputConfig:
    output_dir: str         # Directory to write output files
    output_file_format: str # Output file format (default:parquet) (Optional)
    spark_options: dict  # Spark write options (Optional)
    no_of_partitions: int = -1 # Number of partitions for output (-1 for default partitions) (Optional)
```

### DatasetConfig

Groups together the configs for a single dataset comparison.

```python
from dataclasses import dataclass

@dataclass
class DatasetConfig:
    params: DatasetParams              # Dataset parameters
    source_config: DataframeConfig     # Source DataFrame config
    target_config: DataframeConfig     # Target DataFrame config
```

### ComparisonJobConfig

Top-level config for a comparison job (can include multiple datasets).

```python
from dataclasses import dataclass

@dataclass
class ComparisonJobConfig:
    job_name: str                      # Name of the comparison job
    dataset_configs: list[DatasetConfig] # List of dataset configs to compare
    output_config: OutputConfig        # Output config for all results
```

---

## Output Files

After running a comparison job, the following files/directories are generated under the specified `output_dir` and `job_name`:

### **overall_test_report**

Summary DataFrame with row counts, matched counts, duplicate counts, missing rows, and test status for each dataset. Output will generate under `<output_dir>/<job_name>/overall_test_report`

| dataset_name | count                | matched_count | duplicate_count         | missing_rows           | test_status |
|--------------|----------------------|---------------|------------------------|------------------------|-------------|
| table1       | {"source": 100, "target": 98} | 97            | {"source": 0, "target": 1} | {"source": 1, "target": 3} | PASSED      |

---

### **col_lvl_test_report**

Column-level report showing the count of unmatched values for each non-key column. Output will generate under `<output_dir>/<job_name>/col_lvl_test_report`

| dataset_name | column_name | unmatched_rows_count |
|--------------|-------------|---------------------|
| table1       | colA        | 2                   |
| table1       | colB        | 0                   |

---

### **row_lvl_test_report**

Row-level report with primary keys, duplicate count, missing row status, and match status for each row. Output will generate under `<output_dir>/<job_name>/row_lvl_test_report`

| dataset_name | id | duplicate_count | missing_row_status   | all_rows_matched |
|--------------|----|----------------|----------------------|------------------|
| table1       | 1  | 0              | PRESENT_IN_BOTH      | true             |
| table1       | 2  | 0              | MISSING_AT_TARGET    | false            |

---

### **unmatched_rows/**

Directory containing one file per column with all rows where that column did not match between source and target. Output will generate under `<output_dir>/<job_name>/unmatched_rows/<dataset_name>/<column_name>`

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