"""
跨工具 provider 顺序：与自动降级链并存，可选将某一来源提前尝试。

约定：preference 取值 auto（默认）| eastmoney | sina | csindex | cninfo | ths | standard
（大小写不敏感，em/dongcai 等常见别名会归一化到 eastmoney）。
"""

from __future__ import annotations

from typing import Any, List, Tuple, TypeVar

T = TypeVar("T")

_ALIASES = {
    "": "auto",
    "auto": "auto",
    "em": "eastmoney",
    "eastmoney": "eastmoney",
    "dongcai": "eastmoney",
    "dc": "eastmoney",
    "东财": "eastmoney",
    "sina": "sina",
    "新浪": "sina",
    "csindex": "csindex",
    "中证": "csindex",
    "cninfo": "cninfo",
    "巨潮": "cninfo",
    "ths": "ths",
    "tonghuashun": "ths",
    "同花顺": "ths",
    "standard": "standard",
    "default": "standard",
}


def normalize_provider_preference(raw: str | None) -> str:
    s = (raw or "auto").strip().lower()
    if not s or s == "auto":
        return "auto"
    if s in _ALIASES:
        return _ALIASES[s]
    if s in set(_ALIASES.values()):
        return s
    # 未收录的 tag 原样保留，便于调用方自定义 provider 标签并参与 reorder
    return s


def reorder_provider_chain(
    preference: str | None,
    tagged: List[Tuple[str, T]],
) -> List[Tuple[str, T]]:
    """
    tagged: [(provider_tag, item), ...]
    非 auto 时：将匹配 tag 的项整体移到队首，其余保持相对顺序。
    """
    pref = normalize_provider_preference(preference)
    if pref in ("auto",):
        return list(tagged)
    pref_match = [x for x in tagged if x[0] == pref]
    rest = [x for x in tagged if x[0] != pref]
    return pref_match + rest
