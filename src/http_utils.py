from __future__ import annotations

import random
from typing import Any, Iterable, List, Optional


def get_random_user_agent(cfg: Optional[dict[str, Any]], *, default_ua: str) -> str:
    """
    从配置中获取并随机选择 User-Agent。

    支持的配置字段（兼容历史命名）：
    - `user_agents`: list[str]
    - `user_agent_list`: list[str]
    - `user_agent`: str

    当配置缺失或列表为空时，回退到 default_ua。
    """
    if not cfg:
        return default_ua

    # Prefer explicit UA lists
    for key in ("user_agents", "user_agent_list", "user_agents_list"):
        v = cfg.get(key)
        if isinstance(v, list):
            pool = [str(x).strip() for x in v if str(x).strip()]
            if pool:
                return random.choice(pool)

    # Fallback to single UA
    single = cfg.get("user_agent")
    if isinstance(single, str) and single.strip():
        return single.strip()

    return default_ua

