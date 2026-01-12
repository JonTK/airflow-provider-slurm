"""Setup configuration for airflow-provider-slurm."""

from setuptools import find_packages, setup

from airflow_provider_slurm.version import __version__  # type: ignore[import]

# Read long description from README
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="airflow-provider-slurm",
    version=__version__,
    author="Jon TK",
    author_email="",  # Add your email if desired
    description="Slurm executor for Apache Airflow using REST API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/JonTK/airflow-provider-slurm",
    packages=find_packages(exclude=["tests*"]),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Intended Audience :: System Administrators",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Scientific/Engineering",
        "Topic :: System :: Distributed Computing",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    install_requires=[
        "apache-airflow>=2.5.0",
        "requests>=2.28.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-mock>=3.10.0",
            "pytest-cov>=4.0.0",
            "black>=22.0.0",
            "isort>=5.10.0",
            "flake8>=5.0.0",
            "mypy>=0.990",
            "pre-commit>=2.20.0",
            "responses>=0.22.0",  # For mocking HTTP requests in tests
        ],
    },
    entry_points={
        "airflow.executors": [
            "slurm = airflow_provider_slurm.slurm_executor:SlurmExecutor",
        ],
    },
)
