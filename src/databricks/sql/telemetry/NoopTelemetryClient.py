class NoopTelemetryClient:
    # A no-operation telemetry client that implements the same interface but does nothing

    def export_event(self, event):
        pass

    def flush(self):
        pass

    def close(self):
        pass
