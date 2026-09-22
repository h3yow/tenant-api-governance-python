"""测试公共配置。

统一打开治理日志，确保单测输出中包含 **幂等键、租户标识与判定依据**
（basis 字段），便于人工核对每个判定是如何做出的。
"""

from __future__ import annotations

import logging
import sys

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(
    logging.Formatter("[%(levelname)s] %(name)s %(message)s")
)
_root = logging.getLogger("governance")
_root.handlers.clear()
_root.addHandler(handler)
_root.setLevel(logging.INFO)
_root.propagate = False
