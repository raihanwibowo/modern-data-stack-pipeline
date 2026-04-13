"""
conftest.py — pytest configuration for airflow unit tests.

Adds dags/ to sys.path so that 'packages.*' imports resolve correctly
when running pytest from the airflow/ directory.
"""
import sys
import os

# Make `packages` importable without installing anything
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'dags'))
