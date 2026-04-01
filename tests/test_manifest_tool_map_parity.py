"""Manifest 中的每个 tool id 必须在 tool_runner.TOOL_MAP 中有映射。"""

import importlib.util
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANIFEST_JSON = ROOT / "config" / "tools_manifest.json"
TOOL_RUNNER_PY = ROOT / "tool_runner.py"


def _load_tool_runner():
    spec = importlib.util.spec_from_file_location("tool_runner_parity", TOOL_RUNNER_PY)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


class TestManifestToolMapParity(unittest.TestCase):
    def test_all_manifest_tools_in_tool_map(self):
        data = json.loads(MANIFEST_JSON.read_text(encoding="utf-8"))
        tools = data.get("tools") or []
        ids = [t["id"] for t in tools if isinstance(t, dict) and t.get("id")]
        tr = _load_tool_runner()
        # 兼容清单中的「旧名」：通过 ALIASES 映射到 TOOL_MAP 主工具亦可。
        missing = [i for i in ids if i not in tr.TOOL_MAP and i not in getattr(tr, "ALIASES", {})]
        self.assertEqual(
            missing,
            [],
            msg=f"tools_manifest.json 中有 {len(missing)} 个 id 未在 TOOL_MAP/ALIASES: {missing[:20]}",
        )


if __name__ == "__main__":
    unittest.main()
