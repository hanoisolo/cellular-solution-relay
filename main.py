"""Railway entrypoint shim.

Railway's default Python start command is `uvicorn main:app`, and the
railway.toml startCommand did not take effect on CLI deploys, so this
module re-exports the FastAPI app from server.py.
"""

from server import app

__all__ = ["app"]
