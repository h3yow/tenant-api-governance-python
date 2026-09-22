"""兼容原引导入口：uvicorn main:app。"""

from app.main import app

__all__ = ["app"]
