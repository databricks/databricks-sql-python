from typing import Tuple, Optional

from databricks.sql.exc import ProgrammingError

# Constants
VOLUME_PATH_TEMPLATE = "/Volumes/{catalog}/{schema}/{volume}/"
DIRECTORY_NOT_FOUND_ERROR = "No such file or directory"


def validate_volume_inputs(catalog: str, schema: str, volume: str, path: str, session_id_hex: Optional[str] = None) -> None:
    if not all([catalog, schema, volume, path]):
        raise ProgrammingError(
            "All parameters (catalog, schema, volume, path) are required",
            session_id_hex=session_id_hex
        )


def parse_path(path: str) -> Tuple[str, str]:
    if not path or path == '/':
        return '', ''
    
    # Handle trailing slash - treat "dir-1/" as looking for directory "dir-1"
    path = path.rstrip('/')
    
    if '/' in path:
        folder, filename = path.rsplit('/', 1)
    else:
        folder, filename = '', path
    return folder, filename


def escape_path_component(component: str) -> str:
    """Escape path component to prevent SQL injection.
    """
    return component.replace("'", "''")


def build_volume_path(catalog: str, schema: str, volume: str, folder: str = "") -> str:
    catalog_escaped = escape_path_component(catalog)
    schema_escaped = escape_path_component(schema)
    volume_escaped = escape_path_component(volume)
    volume_path = VOLUME_PATH_TEMPLATE.format(
        catalog=catalog_escaped,
        schema=schema_escaped,
        volume=volume_escaped
    )
    if folder:
        folder_escaped = escape_path_component(folder)
        volume_path += folder_escaped + "/"
    return volume_path


def names_match(found_name: str, target_name: str, case_sensitive: bool) -> bool:
    if case_sensitive:
        return found_name == target_name
    return found_name.lower() == target_name.lower() 