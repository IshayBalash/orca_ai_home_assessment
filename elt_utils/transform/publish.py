class Publish:
    """
    Responsible for writing processed delta records to the target tables.
    """

    @staticmethod
    def publish():
        """Write qualifying records from the delta to target tables."""
