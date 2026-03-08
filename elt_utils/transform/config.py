class Config:
    """Loads and validates the pipeline config.yaml. All pipeline behaviour is driven from here."""

    def __init__(self, config_path: str):
        """Parse config.yaml at config_path into an accessible config dict."""

    @property
    def configs(self) -> dict:
        """Return the full config dict."""
