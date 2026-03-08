def run_pipe():
    """
    Entry point for the pipeline. Orchestrates all stages in order:
    init → delta → dqa → bl → publish
    """
