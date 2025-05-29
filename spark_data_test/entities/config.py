from dataclasses import dataclass, field
from spark_data_test.constants.common_constants import PARQUET_FMT

@dataclass
class DatasetParams:
    dataset_name: str
    primary_keys: list
    select_cols: list = field(default_factory=lambda: ["*"])
    drop_cols: list = field(default_factory=list)

    def __post_init__(self):
        self.validate()

    def validate(self):
        if not self.dataset_name:
            raise ValueError("job_name is required in DatasetParams")
        if not self.primary_keys or not isinstance(self.primary_keys, list) or not all(self.primary_keys):
            raise ValueError("primary_keys (non-empty list) is required in DatasetParams")

@dataclass
class OutputConfig:
    output_dir: str
    output_file_format: str = PARQUET_FMT
    spark_options: dict = field(default_factory=dict)
    no_of_partitions: int = -1  # -1 means default partitioning

    def __post_init__(self):
        self.validate()

    def validate(self):
        if not self.output_dir:
            raise ValueError("output_dir is required in OutputConfig")
        
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
