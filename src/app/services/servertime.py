"""Simple ping/pong style service returning the server's time."""
from datetime import datetime


def factory(app):
    """Generate."""

    @app.get("/servertime", description=__doc__)
    def time_service():
        """Babysteps."""
        return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    time_service.__doc__ = __doc__
