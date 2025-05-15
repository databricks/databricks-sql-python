from databricks.sql import Connection

class VolumeProcessor:
    def __init__(self, **kwargs):
        self.connection = kwargs.get("connection")
        self.local_path = kwargs.get("local_path")
        self.input_stream = kwargs.get("input_stream")
        self.content_length = kwargs.get("content_length")
        self.abs_staging_allowed_local_paths = self.get_abs_staging_allowed_local_paths(kwargs.get("staging_allowed_local_path"))
        
    def process_volume(self, catalog: str, schema: str, volume: str):
        pass
