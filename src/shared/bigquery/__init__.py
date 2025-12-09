"""
Shared BigQuery utilities.
Chứa BigQuery client và các utilities chung.
"""
from .client import BigQueryClient
from .external_tables import BigQueryExternalTableSetup

__all__ = ['BigQueryClient', 'BigQueryExternalTableSetup']
