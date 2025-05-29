import pytest
from spark_data_test.utils.config_reader import parse_comparison_job_config

def test_parse_comparison_job_config():
    try:
        parse_comparison_job_config({})
        assert False
    except Exception:
        assert True