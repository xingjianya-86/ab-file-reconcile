"""file-reconcile 的单元测试。

只测纯逻辑与安全闸门，不碰网络、不碰真实的 data.db：CI 里既没有
qBittorrent 也没有 AutoBangumi 的数据目录，任何依赖它们的用例都跑不过。
"""

import asyncio
from pathlib import Path

import pytest
from ab_sdk import points
from ab_sdk.hooks import PROVIDER_ATTR
from ab_sdk.manifest import check
from ab_sdk.tasks import ScheduledTask
from ab_sdk.testing import create_plugin
from fastapi import APIRouter
from pydantic import ValidationError

import file_reconcile
from file_reconcile import (
    FileReconcile,
    Finding,
    Options,
    _find_elsewhere,
    _safe_size,
    _track_bangumi,
    episode_of,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


# ------------------------------------------------------------------ 集数解析


def test_episode_from_release_name() -> None:
    assert episode_of(
        "[LoliHouse] Tenbin - 09 [WebRip 1080p HEVC-10bit AAC ASSx2].mkv"
    ) == 9


def test_episode_ignores_version_suffix() -> None:
    # v2 重发的第 9 集不能被解析成别的集数，否则会误判为「缺 9 缺 9v2」
    name = (
        "[LoliHouse] Yuusha Party ni Kawaii Ko ga Ita node, Kokuhaku shitemita. "
        "- 09v2 [WebRip 1080p HEVC-10bit AAC SRTx2].mkv"
    )
    assert episode_of(name) == 9


def test_episode_from_mikan_rss_title() -> None:
    # AB 存的是 RSS 标题（含完整目录路径），与磁盘文件名不同
    rss_title = (
        "[LoliHouse] 转校生美少女竟是我曾经认为是男孩子的青梅竹马 / "
        "转校先の清楚可憐な美少女が… / てんびん - 11 "
        "[WebRip 1080p HEVC-10bit AAC ASSx2]"
    )
    assert episode_of(rss_title) == 11


def test_episode_falls_back_to_e_and_bracket_forms() -> None:
    assert episode_of("[Group] Show E07 [1080p].mkv") == 7
    assert episode_of("[Group] Show [07] [1080p].mkv") == 7


def test_movie_without_episode_returns_none() -> None:
    movie = (
        "[Studio GreenTea] Kobayashi-san Chi no Maidragon "
        "[Movie v2][BDRip][HEVC-10bit 1080p FLAC ASSx2].mkv"
    )
    assert episode_of(movie) is None


def test_episode_does_not_read_resolution_as_episode() -> None:
    # 1080p / HEVC-10bit 里的数字不是集数
    assert episode_of("[Group] Sample [WebRip 1080p HEVC-10bit AAC].mkv") is None


def test_episode_of_empty_is_none() -> None:
    assert episode_of(None) is None
    assert episode_of("") is None


# ------------------------------------------------------------------ 路径映射


def test_to_local_maps_downloader_prefix() -> None:
    plugin, _ = create_plugin(
        FileReconcile,
        {"downloader_prefix": "/downloads", "local_prefix": "/media/我的番剧"},
    )
    assert plugin.to_local("/downloads") == "/media/我的番剧"
    assert (
        plugin.to_local("/downloads/某番剧/Season 1/x.mkv")
        == "/media/我的番剧/某番剧/Season 1/x.mkv"
    )


def test_to_local_returns_none_for_foreign_path() -> None:
    plugin, _ = create_plugin(FileReconcile, {})
    # 前缀对不上时不能瞎猜：返回 None 让调用方报 path_unmapped 而不是误判缺失
    assert plugin.to_local("/somewhere/else/x.mkv") is None


def test_to_local_does_not_match_partial_prefix() -> None:
    plugin, _ = create_plugin(
        FileReconcile,
        {"downloader_prefix": "/downloads", "local_prefix": "/media/我的番剧"},
    )
    # /downloads-old 不是以 /downloads/ 开头，不能被当成同一棵树
    assert plugin.to_local("/downloads-old/x.mkv") is None


def test_options_reject_relative_paths() -> None:
    with pytest.raises(ValidationError):
        Options(local_prefix="media/我的番剧")
    with pytest.raises(ValidationError):
        Options(downloader_prefix="downloads")


def test_options_default_interval_is_positive() -> None:
    opts = Options()
    assert opts.interval_hours >= 0.25
    assert opts.max_fix_per_run >= 0


# ------------------------------------------------------------------ 文件探测


def test_safe_size_missing_file_is_none(tmp_path: Path) -> None:
    assert _safe_size(str(tmp_path / "nope.mkv")) is None


def test_safe_size_returns_bytes(tmp_path: Path) -> None:
    target = tmp_path / "a.mkv"
    target.write_bytes(b"0123456789")
    assert _safe_size(str(target)) == 10


def test_find_elsewhere_locates_moved_file(tmp_path: Path) -> None:
    moved = tmp_path / "Photos" / "某番剧"
    moved.mkdir(parents=True)
    (moved / "Tenbin - 09.mkv").write_bytes(b"x")
    # 期望的位置没有，但别处有 -> 报告「被移动到哪了」而不是直接补下
    assert _find_elsewhere([str(tmp_path)], "Tenbin - 09.mkv") == str(
        moved / "Tenbin - 09.mkv"
    )


def test_find_elsewhere_returns_none_when_absent(tmp_path: Path) -> None:
    (tmp_path / "empty").mkdir()
    assert _find_elsewhere([str(tmp_path)], "missing.mkv") is None


def test_find_elsewhere_skips_missing_roots(tmp_path: Path) -> None:
    assert _find_elsewhere([str(tmp_path / "not-there")], "x.mkv") is None
    assert _find_elsewhere([], "x.mkv") is None


# ------------------------------------------------------------------ qB 侧关联


def test_track_bangumi_reads_tag_and_task_name() -> None:
    eps: dict[int, set[int]] = {}
    task = {"tags": "ab:8, Bangumi", "name": "[LoliHouse] Tenbin - 03 [1080p].mkv"}
    _track_bangumi(eps, task)
    assert eps == {8: {3}}


def test_track_bangumi_uses_filenames_for_multifile_tasks() -> None:
    eps: dict[int, set[int]] = {}
    # 多文件任务的任务名是目录名，没有集数；必须从文件名里取
    task = {
        "tags": "ab:8",
        "name": "[LoliHouse] Tenbin Season 1",
        "files": [{"name": "Tenbin - 04 [1080p].mkv"}],
    }
    _track_bangumi(eps, task, task["files"])
    assert eps == {8: {4}}


def test_track_bangumi_ignores_tasks_without_tag() -> None:
    eps: dict[int, set[int]] = {}
    _track_bangumi(eps, {"tags": "", "name": "x - 01 [1080p].mkv"})
    _track_bangumi(eps, {"tags": "Bangumi", "name": "y - 02 [1080p].mkv"})
    assert eps == {}


# ------------------------------------------------------------------ 扩展点登记


def test_manifest_is_valid_and_sdk_compatible() -> None:
    manifest, problems = check(REPO_ROOT)
    assert problems == [], problems
    assert manifest is not None
    assert manifest.id == "file-reconcile"
    assert manifest.sdk_compatible()


def test_scheduled_task_provider_registered() -> None:
    spec = getattr(FileReconcile.reconcile_task, PROVIDER_ATTR)
    assert spec.point == points.SCHEDULED_TASK
    assert spec.id == "file-reconcile"


def test_api_router_provider_registered() -> None:
    spec = getattr(FileReconcile.api, PROVIDER_ATTR)
    assert spec.point == points.API_ROUTER
    assert spec.id == "api"


def test_scheduled_task_spec_is_runnable() -> None:
    plugin, _ = create_plugin(FileReconcile, {"interval_hours": 6})
    spec = plugin.reconcile_task()
    assert isinstance(spec, ScheduledTask)
    import inspect

    assert inspect.iscoroutinefunction(spec.run)
    assert float(spec.interval()) == 6 * 3600
    assert spec.initial_delay > 0
    # 关掉自动修复不影响对账本身：任务照跑，只是不动任何东西
    assert spec.enabled is None or spec.enabled()


def test_api_router_exposes_report_and_check() -> None:
    plugin, _ = create_plugin(FileReconcile, {})
    router = plugin.api()
    assert isinstance(router, APIRouter)
    paths = {getattr(r, "path", None) for r in router.routes}
    assert paths == {"/report", "/check"}


# ------------------------------------------------------------------ 安全闸门


def _unreadable_root_options(**overrides: object) -> dict[str, object]:
    """下载目录与数据库都指向不存在的位置：CI 里一定读不到。"""
    base: dict[str, object] = {
        "auto_fix": True,  # 故意开着，验证闸门优先于修复开关
        "local_prefix": "/definitely/not/here",
        "downloader_prefix": "/downloads",
        "search_roots": [],
        "db_path": "/definitely/not/here.db",
        "qb_host": "",  # 走 config.json，CI 里也不存在
    }
    base.update(overrides)
    return base


def test_unreadable_root_blocks_all_fixes() -> None:
    """下载目录读不到时必须整轮禁修——否则路径配错会触发海量误补下。"""
    plugin, _ = create_plugin(FileReconcile, _unreadable_root_options())
    report = asyncio.run(plugin.run_once())

    summary = report["summary"]
    assert report["download_root_readable"] is False
    assert summary["fix_attempted"] == 0, "闸门失效：读不到目录却尝试了修复"
    assert summary["fixed"] == 0
    assert any("下载目录不可读" in e for e in summary["errors"])
    # 检测出的目录缺失只报告，不带修复动作
    assert all(not f["fixable"] for f in report["findings"])


def test_run_once_returns_json_serialisable_report() -> None:
    plugin, _ = create_plugin(FileReconcile, _unreadable_root_options())
    report = asyncio.run(plugin.run_once())
    import json

    text = json.dumps(report, ensure_ascii=False)
    assert "generated_at" in report
    assert "summary" in report
    assert "findings" in report
    assert '"errors"' in text


def test_auto_fix_off_never_attempts_repairs() -> None:
    plugin, _ = create_plugin(
        FileReconcile, _unreadable_root_options(auto_fix=False)
    )
    report = asyncio.run(plugin.run_once())
    assert report["auto_fix"] is False
    assert report["summary"]["fix_attempted"] == 0


def test_report_is_cached_in_kv_for_the_rest_endpoint() -> None:
    plugin, ctx = create_plugin(FileReconcile, _unreadable_root_options())
    asyncio.run(plugin.run_once())

    async def read_back():
        return await ctx.kv.get("last_report")

    stored = asyncio.run(read_back())
    assert stored is not None
    assert stored["summary"]["fix_attempted"] == 0


# ------------------------------------------------------------------ 数据结构


def test_finding_defaults_are_safe() -> None:
    finding = Finding(kind="qb_file_missing", severity="error", path="/x")
    # 默认不可修复：只有调用方显式判断后才置 True，避免误伤
    assert finding.fixable is False
    assert finding.fixed is False
    assert finding.fix_action is None
    assert finding.found_elsewhere is None


def test_module_does_not_import_host_internals() -> None:
    """插件只能依赖 ab_sdk；import module.* 会让升级随时打断。

    用 AST 判断而不是字符串匹配：模块的文档字符串里就写着 ``import module.*``
    这句话，按原文找会误报。
    """
    import ast

    tree = ast.parse(
        (REPO_ROOT / "file_reconcile.py").read_text(encoding="utf-8")
    )
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.append(node.module)
    offenders = [name for name in imported if name == "module" or name.startswith("module.")]
    assert offenders == [], f"插件不应依赖宿主内部模块：{offenders}"


def test_entry_module_matches_manifest() -> None:
    manifest, _ = check(REPO_ROOT)
    assert manifest is not None
    assert manifest.entry_module == "file_reconcile"
    assert getattr(file_reconcile, manifest.entry_class) is not None
