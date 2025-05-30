from spark_data_test.utils.config_reader import parse_comparison_job_config

def test_parse_comparison_job_config_exception():
    try:
        parse_comparison_job_config(None)
        assert False
    except ValueError as e:
        assert True


def test_parse_comparison_job_config():
    config = parse_comparison_job_config({
    "job_name": "test_job",
    "dataset_configs": [
        {
    "source_config": {
        "path": "./df-validator/input/df",
        "file_format": "csv",
        "spark_options": {"header": "true"}
        },
            "target_config": {
        "path": "./df-validator/input/df2",
        "file_format": "csv",
        "spark_options": {"header": "true"}
        },
        "params": {"dataset_name": "test_job", "primary_keys": ["id"]}}],
    "output_config": {
        "output_dir": "./df-validator/test_output",
        "output_file_format": "parquet",
        "spark_options": {"compression": "snappy"},
        "no_of_partitions": -1
}})
    assert config.job_name == "test_job"
    assert len(config.dataset_configs) == 1