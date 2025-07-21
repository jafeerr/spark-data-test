from dataclasses import dataclass, field
from spark_data_test.constants.common_constants import PARQUET_FMT

@dataclass
class TestParams:
    difference_tolerance: float = 0.0

@dataclass
class DatasetParams:
    dataset_name: str
    primary_keys: list
    test_params: TestParams = field(default_factory=TestParams)
    select_cols: list = field(default_factory=lambda: ["*"])
    drop_cols: list = field(default_factory=list)
    

@dataclass
class OutputConfig:
    output_dir: str
    output_file_format: str = PARQUET_FMT
    spark_options: dict = field(default_factory=dict)
    no_of_partitions: int = -1  # -1 means default partitioning
        
@dataclass
class DataframeConfig:
    path: str
    file_format: str = PARQUET_FMT
    spark_options: dict = field(default_factory=dict)        


@dataclass
class DatasetConfig:
    params: DatasetParams
    source_config: DataframeConfig
    target_config: DataframeConfig

@dataclass
class ComparisonJobConfig:
    job_name: str
    dataset_configs: list[DatasetConfig]
    output_config: OutputConfig
