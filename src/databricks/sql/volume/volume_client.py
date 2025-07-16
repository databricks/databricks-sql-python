
import logging
import requests

from databricks.sql.exc import OperationalError, ProgrammingError

logger = logging.getLogger(__name__)


class VolumeClient:

    # API endpoint constant
    _DIRECTORIES_ENDPOINT = "/api/2.0/fs/directories"
    
    def __init__(self, auth_provider, host_url: str, session_id_hex: str):
        """
        Initialize the VolumeClient.
        
        Args:
            auth_provider: The authentication provider for API requests
            host_url: The Databricks workspace URL
            session_id_hex: The session ID for error reporting
        """
        self.auth_provider = auth_provider
        self.host_url = host_url if host_url.startswith(('http://', 'https://')) else f"https://{host_url}"
        self.host_url = self.host_url.rstrip('/')
        self.session_id_hex = session_id_hex
    
    def _get_auth_headers(self) -> dict:
        headers = {'Content-Type': 'application/json'}
        self.auth_provider.add_headers(headers)
        return headers
    
    def _build_volume_path(self, catalog: str, schema: str, volume: str, path: str) -> str:
        return f"/Volumes/{catalog}/{schema}/{volume}/{path.lstrip('/')}"
    
    def _get_base_name_from_path(self, path: str) -> str:
        return path[path.rfind("/") + 1:]
    
    def _get_parent_path(self, path: str) -> str:
        return path[:path.rfind("/")] if "/" in path else ""
    
    def _get_directory_contents(self, catalog: str, schema: str, volume: str, parent_path: str) -> list:
        """Get directory contents from the API."""
        parent_volume_path = self._build_volume_path(catalog, schema, volume, parent_path)
        url = f"{self.host_url}{self._DIRECTORIES_ENDPOINT}{parent_volume_path}"
        
        response = requests.get(url, headers=self._get_auth_headers())
        
        if response.status_code == 404:
            return []
        elif response.status_code != 200:
            raise OperationalError(
                f"Failed to list directory contents: {response.status_code} - {response.text}",
                session_id_hex=self.session_id_hex
            )
        
        return response.json().get('contents', [])
    
    def object_exists(self, catalog: str, schema: str, volume: str, path: str, case_sensitive: bool = True) -> bool:
        """
        Check if an object exists in the volume.
        
        Args:
            catalog: The catalog name
            schema: The schema name  
            volume: The volume name
            path: The path within the volume
            case_sensitive: Whether to perform case-sensitive matching
            
        Returns:
            True if object exists, False otherwise
            
        Raises:
            ProgrammingError: If parameters are invalid
            OperationalError: If the API request fails
        """
        if not all([catalog, schema, volume, path]):
            raise ProgrammingError(
                "All parameters (catalog, schema, volume, path) are required",
                session_id_hex=self.session_id_hex
            )
        
        if not path.strip():
            return False
            
        try:
            base_name = self._get_base_name_from_path(path)
            parent_path = self._get_parent_path(path)
            files = self._get_directory_contents(catalog, schema, volume, parent_path)
            
            for file_info in files:
                file_name = file_info.get('name', '')
                name_match = (file_name == base_name if case_sensitive 
                             else file_name.lower() == base_name.lower())
                if name_match:
                    return True
            
            return False
            
        except requests.exceptions.RequestException as e:
            raise OperationalError(
                f"Network error while checking object existence: {str(e)}",
                session_id_hex=self.session_id_hex
            ) from e
        except Exception as e:
            logger.error(
                f"Error checking object existence: {catalog}/{schema}/{volume}/{path}",
                exc_info=True
            )
            raise OperationalError(
                f"Error checking object existence: {str(e)}",
                session_id_hex=self.session_id_hex
            ) from e 