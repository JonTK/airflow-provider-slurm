"""Airflow Slurm Executor - Execute Airflow tasks on Slurm clusters via REST API."""

__version__ = "0.1.0"
__author__ = "Jon TK"

from airflow_slurm_executor.slurm_executor import SlurmExecutor

__all__ = ["SlurmExecutor"]