
import logging
from typing import TYPE_CHECKING, List

from databricks.sql.exc import OperationalError, ProgrammingError, ServerOperationError
from .volume_utils import (
    parse_path,
    build_volume_path,
    names_match,
    validate_volume_inputs,
    DIRECTORY_NOT_FOUND_ERROR
)

# Avoid circular import
if TYPE_CHECKING:
    from databricks.sql.client import Connection

logger = logging.getLogger(__name__)


class UCVolumeClient:
    
    def __init__(self, connection: "Connection"):
        self.connection = connection
        self.session_id_hex = connection.get_session_id_hex()
    
    
    def _execute_list_query(self, query: str) -> List:
        """Execute LIST query and handle common errors."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()
        except ServerOperationError as e:
            if DIRECTORY_NOT_FOUND_ERROR in str(e):
                return []  # Directory doesn't exist
            raise OperationalError(f"Query failed: {str(e)}", session_id_hex=self.session_id_hex) from e
        except Exception as e:
            raise OperationalError(f"Query failed: {str(e)}", session_id_hex=self.session_id_hex) from e
    
    def object_exists(self, catalog: str, schema: str, volume: str, path: str, case_sensitive: bool = True) -> bool:

        validate_volume_inputs(catalog, schema, volume, path, self.session_id_hex)
        
        if not path.strip():
            return False
        
        folder, filename = parse_path(path)        
        volume_path = build_volume_path(catalog, schema, volume, folder)        
        query = f"LIST '{volume_path}'"
        logger.debug(f"Executing query: {query}")
        
        results = self._execute_list_query(query)
        if not results:
            return False
        
        # Check if our file exists in results
        # Row structure: [path, name, size, modification_time]
        # Example: ['/Volumes/catalog/schema/volume/dir/file.txt', 'file.txt', 1024, 1752757716901]
        # For directories: both path and name end with '/' (e.g., '/Volumes/.../dir/', 'dir/')
        for row in results:
            if len(row) > 1:
                found_name = str(row[1])  # Second column is the filename
                
                # Remove trailing slash from directories
                if found_name.endswith('/'):
                    found_name = found_name[:-1]
                
                if names_match(found_name, filename, case_sensitive):
                    return True
        
        return False 