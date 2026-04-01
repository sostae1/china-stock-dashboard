import importlib.util
import json
import subprocess
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TOOL_RUNNER_PY = ROOT / "tool_runner.py"


def _load_tool_runner_module():
    spec = importlib.util.spec_from_file_location("tool_runner_test_module", TOOL_RUNNER_PY)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def _run_tool(tool_name: str, params: dict):
    args_json = json.dumps(params, ensure_ascii=False)
    proc = subprocess.run(
        [sys.executable, str(TOOL_RUNNER_PY), tool_name, args_json],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
    )
    out = (proc.stdout or "").strip()
    if not out:
        raise AssertionError(f"tool_runner produced no stdout (rc={proc.returncode}): {proc.stderr}")
    return json.loads(out)


class TestToolRunnerDispatch(unittest.TestCase):
    def test_tool_map_retarget_to_wrapper(self):
        module = _load_tool_runner_module()
        self.assertEqual(module.TOOL_MAP["tool_fetch_market_data"].module_path, "data.fetch_market_data")
        self.assertEqual(module.TOOL_MAP["tool_read_market_data"].module_path, "data.read_market_data")

    def test_tool_fetch_market_data_missing_asset_type(self):
        res = _run_tool(
            "tool_fetch_market_data",
            {
                "asset_type": "",
                "view": "",
            },
        )
        self.assertFalse(res.get("success", True))
        self.assertIn("asset_type", res.get("message", ""))

    def test_tool_fetch_market_data_unsupported_view(self):
        res = _run_tool(
            "tool_fetch_market_data",
            {
                "asset_type": "index",
                "asset_code": "000001",
                "view": "bad",
            },
        )
        self.assertFalse(res.get("success", True))
        self.assertIn("不支持 index.view=bad", res.get("message", ""))

    def test_tool_fetch_market_data_stock_market_overview_no_asset_code(self):
        res = _run_tool(
            "tool_fetch_market_data",
            {
                "asset_type": "stock",
                "asset_code": "",
                "view": "market_overview",
            },
        )
        # 联网失败时 success 为 false；结构应含 message 键
        self.assertIn("message", res)
        self.assertIn("success", res)


if __name__ == "__main__":
    unittest.main()

