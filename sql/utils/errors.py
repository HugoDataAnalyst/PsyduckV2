class FlushError(RuntimeError):
    """
    A bulk SQL write did not complete, so the caller must keep its batch.

    `remaining` carries the rows that were NOT written when a write commits in
    parts (shiny rates upsert chunk by chunk). It is None when nothing from the
    batch was committed, and callers then retry the whole batch.
    """

    def __init__(self, message: str, remaining: list | None = None):
        super().__init__(message)
        self.remaining = remaining
