import setuptools
import os
version = '0.1.0'
try:
    version =os.environ['APP_VERSION']
except KeyError:
    pass
setuptools.setup(
    name="spark_data_test",
    description="A library for validating and comparing datasets in Spark using PySpark.",
    author="Jafeer Ali",
    author_email="jafeeralin@gmail.com",
    license="MIT",
    version=version,
    packages=["spark_data_test"],
    install_requires=[
        'dacite>=v1.9.2'
    ],
    extras_require={
        "dev": ["pytest>=5", "pytest-cov"]
    }
    python_requires='>=3.7',
)