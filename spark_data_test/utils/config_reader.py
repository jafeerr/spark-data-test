from spark_data_test.entities.config import ComparisonJobConfig
from dacite import from_dict

def parse_comparison_job_config(config):
    if not config:
        raise ValueError("Configuration dictionary cannot be empty")
    
    return from_dict(data_class = ComparisonJobConfig, data=config)


def dict_to_dataclass(data_class, data_dict):
    return from_dict(data_class = data_class, data=data_dict)