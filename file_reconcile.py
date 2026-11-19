"""file-reconcile：下载任务与磁盘文件对账。

对账关系（两种）：

1. **qBittorrent 任务 ↔ 磁盘**：qB 里每个任务的每个文件，按
   ``save_path + files[].name`` 拼出路径去磁盘上 stat，核对「存在」与「字节数」。
2. **AutoBangumi 已下载标记 ↔ 磁盘**：``torrent.downloaded = 1`` 的记录，
   按集数在该番剧的 ``save_path`` 目录里找对应文件。

发现问题后的自动修复**只走 qB API**，既不改 ``data.db``，也不写媒体文件：

- 文件缺失但 qB 还留着任务 → 触发 ``recheck``，由 qB 校验后自己补下；
- AB 标记已下载但 qB 已经没有这个任务 → 用记录里的种子 URL 重新投递。

下载目录以只读方式挂载（compose 中 ``/vol1/1000:/media:ro``），所以插件
**结构上无法**写入或删除媒体文件。

设计要点：

- 不能用 ``qb_hash`` 关联两边——AB 的 ``torrent.qb_hash`` 全是空的，而且
  AB 存的是 RSS 标题、qB 存的是种子内部名，两边对不上。所以按
  「番剧 → save_path → 集数」关联，集数从两边的名字里各解析一次。
- 读 AB 库用 ``mode=ro`` 只读连接 + ``busy_timeout``，与宿主的 SQLAlchemy
  写入互不阻塞（库是 WAL 模式）。
- 所有阻塞 IO（stat、walk、sqlite、qB HTTP 之外的部分）都丢进线程池。
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sqlite3
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx2
from fastapi import APIRouter
from pydantic import BaseModel, Field, field_validator

from ab_sdk import Plugin, PluginContext, points, provider, secret_field
from ab_sdk.tasks import ScheduledTask

# 宿主配置：下载器连接参数在这里（插件不能 import module.*，只能读文件）
CONFIG_JSON = "/app/config/config.json"

# qB 并发拉取文件列表的上限，避免一次性打满下载器
QB_CONCURRENCY = 8

# 集数解析。顺序有意义：先认「 - 09 [」这种明确写法，再退回 E09 / [09]。
# 放在最前的模式必须带分隔符，否则会把 1080p、HEVC-10bit 之类误判成集数。
_EPISODE_RES = (
    re.compile(r"(?:^|[\s/])-\s*(\d{1,3})(?:v\d+)?\s*\["),
    re.compile(r"\bE(\d{1,3})\b"),
    re.compile(r"\[(\d{1,3})\]"),
)


def episode_of(name: str | None) -> int | None:
    """从种子名或文件名里解析集数；解析不出来返回 None（如剧场版）。"""
    if not name:
        return None
    for rx in _EPISODE_RES:
        m = rx.search(name)
        if m:
            return int(m.group(1))
    return None


class Options(BaseModel):
    interval_hours: float = Field(
        default=6, ge=0.25, le=168, title="对账间隔（小时）"
    )
    downloader_prefix: str = Field(
        default="/downloads",
        title="下载器路径前缀",
        description="qB 里 save_path 的开头部分",
    )
    local_prefix: str = Field(
        default="/media/我的番剧",
        title="容器内对应路径",
        description="同一批文件在本容器里看到的路径；两者的其余部分逐字相同",
    )
    search_roots: list[str] = Field(
        default_factory=lambda: ["/media"],
        title="文件去向搜索根目录",
        description="文件不在下载目录时按文件名在这些目录下找，用于定位被移动的文件；留空则不找",
    )
    auto_fix: bool = Field(default=True, title="自动修复")
    fix_recheck: bool = Field(
        default=True, title="文件缺失时让 qB 重新校验并补下"
    )
    fix_redispatch: bool = Field(
        default=True, title="AB 已下载但 qB 无任务时重新投递种子"
    )
    max_fix_per_run: int = Field(
        default=10, ge=0, le=500, title="单次最多修复数"
    )
    instance_id: str = Field(default="default", title="下载器实例 id")
    qb_host: str = Field(default="", title="qB 地址（留空则读取 AB 设置）")
    qb_username: str = Field(default="", title="qB 用户名（留空则读取 AB 设置）")
    qb_password: str = secret_field(
        default="", title="qB 密码（留空则读取 AB 设置）"
    )
    db_path: str = Field(default="/app/data/data.db", title="AB 数据库路径")
    qb_timeout: float = Field(
        default=30, ge=5, le=120, title="qB 请求超时（秒）"
    )

    @field_validator("downloader_prefix", "local_prefix")
    @classmethod
    def _absolute(cls, value: str) -> str:
        value = value.rstrip("/")
        if not value.startswith("/"):
            raise ValueError("须为绝对路径")
        return value


@dataclass
class Finding:
    """一条不一致。``kind`` 决定展示与修复方式。"""

    kind: str  # qb_file_missing / qb_size_mismatch / ab_file_missing /
    #             ab_dir_missing / path_unmapped
    severity: str  # info / warning / error
    path: str
    detail: str = ""
    bangumi: str | None = None
    episode: int | None = None
    torrent_hash: str | None = None
    torrent_url: str | None = None
    bangumi_id: int | None = None
    # 下载器视角的番剧保存目录（重新投递时用它，不能用裸前缀——
    # 否则文件会被丢到下载根目录而不是番剧文件夹）
    save_path: str | None = None
    archived: bool = False
    found_elsewhere: str | None = None
    fixable: bool = False
    fixed: bool = False
    fix_action: str | None = None
    fix_error: str | None = None


class QBClient:
    """qB WebAPI 的最小异步客户端（登录 + 只读查询 + 修复动作）。"""

    def __init__(self, base: str, username: str, password: str, timeout: float):
        self._base = base.rstrip("/")
        self._username = username
        self._password = password
        self._client = httpx2.AsyncClient(timeout=timeout)

    async def __aenter__(self) -> "QBClient":
        resp = await self._client.post(
            f"{self._base}/api/v2/auth/login",
            data={"username": self._username, "password": self._password},
        )
        if resp.status_code >= 400:
            await self._client.aclose()
            raise RuntimeError(f"qB 登录失败：HTTP {resp.status_code}")
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self._client.aclose()

    async def get_json(self, path: str) -> Any:
        resp = await self._client.get(self._base + path)
        if resp.status_code >= 400:
            raise RuntimeError(f"qB GET {path} -> HTTP {resp.status_code}")
        return resp.json()

    async def post_form(self, path: str, data: dict[str, str]) -> tuple[int, str]:
        resp = await self._client.post(self._base + path, data=data)
        return resp.status_code, resp.text


class FileReconcile(Plugin[Options]):
    config_model = Options

    def __init__(self, ctx: PluginContext) -> None:
        super().__init__(ctx)
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------ 路径与数据

    def to_local(self, path: str) -> str | None:
        """下载器路径 → 本容器路径；前缀对不上时返回 None（不猜）。"""
        pre = self.config.downloader_prefix
        if path == pre:
            return self.config.local_prefix
        if path.startswith(pre + "/"):
            return self.config.local_prefix + path[len(pre):]
        return None

    def _qb_conn(self) -> tuple[str, str, str]:
        """(地址, 用户名, 密码)。插件配置留空时读宿主设置。"""
        cfg = self.config
        if cfg.qb_host:
            return cfg.qb_host, cfg.qb_username, cfg.qb_password
        with open(CONFIG_JSON, encoding="utf-8") as fh:
            host_cfg = json.load(fh)
        for inst in host_cfg.get("plugins", {}).get("instances", []):
            if (
                inst.get("id") == cfg.instance_id
                and inst.get("point") == "downloader"
            ):
                opts = inst["options"]
                return opts["host"], opts["username"], opts["password"]
        raise RuntimeError(f"宿主设置里找不到下载器实例 {cfg.instance_id!r}")

    async def _read_ab_rows(self) -> list[dict[str, Any]]:
        """只读拉出「已下载」的种子行 + 所属番剧。丢线程池，避免卡事件循环。"""

        def _query() -> list[dict[str, Any]]:
            uri = f"file:{self.config.db_path}?mode=ro"
            conn = sqlite3.connect(uri, uri=True, timeout=15)
            try:
                conn.row_factory = sqlite3.Row
                conn.execute("pragma busy_timeout=15000")
                rows = conn.execute(
                    """
                    select t.id        as torrent_id,
                           t.name      as torrent_name,
                           t.url       as url,
                           b.id        as bangumi_id,
                           b.official_title as title,
                           b.save_path as save_path,
                           b.deleted   as deleted,
                           b.archived  as archived
                      from torrent t
                      join bangumi b on b.id = t.bangumi_id
                     where t.downloaded = 1
                    """
                ).fetchall()
                return [dict(r) for r in rows]
            finally:
                conn.close()

        return await asyncio.to_thread(_query)

    @staticmethod
    def _find_elsewhere(roots: list[str], filename: str) -> str | None:
        """在其它目录里按文件名找（用于报告「被移动到哪了」）。"""
        for root in roots:
            if not root or not os.path.isdir(root):
                continue
            for dirpath, _dirnames, filenames in os.walk(root):
                if filename in filenames:
                    return os.path.join(dirpath, filename)
        return None

    # ------------------------------------------------------------ 主流程

    async def run_once(self) -> dict[str, Any]:
        """跑一轮对账，返回可 JSON 序列化的报告。并发调用会复用上一轮结果。"""
        if self._lock.locked():
            previous = await self.ctx.kv.get("last_report")
            if previous:
                previous["note"] = "上一轮仍在进行，返回其结果"
                return previous
        async with self._lock:
            report = await self._reconcile()
            await self.ctx.kv.set("last_report", report)
            return report

    async def _reconcile(self) -> dict[str, Any]:
        started = time.monotonic()
        cfg = self.config
        findings: list[Finding] = []
        errors: list[str] = []

        local_root = Path(cfg.local_prefix)
        root_ok = await asyncio.to_thread(local_root.is_dir)
        if not root_ok:
            # 看不到下载目录时一切「缺失」都不可信：只报告，绝不修复
            errors.append(f"下载目录不可读：{cfg.local_prefix}（跳过本轮修复）")

        # --- qB 侧 -------------------------------------------------------
        qb_torrents: list[dict[str, Any]] = []
        qb_files_checked = 0
        qb_ok = 0
        # bangumi_id -> 该番剧在 qB 里已有的集数（用于判断「任务还在不在」）
        qb_eps: dict[int, set[int]] = {}
        # 需要 recheck 的任务 hash（去重）-> 该任务下所有受影响的 finding
        recheck_targets: dict[str, list[Finding]] = {}

        try:
            host, user, password = await asyncio.to_thread(self._qb_conn)
        except Exception as e:
            host = user = password = ""
            errors.append(f"读取 qB 连接参数失败：{e}")

        if host and root_ok:
            try:
                async with QBClient(host, user, password, cfg.qb_timeout) as qb:
                    qb_torrents = await qb.get_json(
                        "/api/v2/torrents/info?filter=all"
                    )
                    sem = asyncio.Semaphore(QB_CONCURRENCY)

                    async def _one(task: dict[str, Any]) -> None:
                        async with sem:
                            files = await qb.get_json(
                                "/api/v2/torrents/files?hash=" + task["hash"]
                            )
                        _track_bangumi(qb_eps, task, files)
                        for f in files:
                            yield_files.append((task, f))

                    yield_files: list[tuple[dict[str, Any], dict[str, Any]]] = []
                    await asyncio.gather(
                        *(_one(t) for t in qb_torrents), return_exceptions=True
                    )

                    for task, f in yield_files:
                        qb_files_checked += 1
                        joined = task["save_path"].rstrip("/") + "/" + f["name"]
                        local = self.to_local(joined)
                        if local is None:
                            findings.append(
                                Finding(
                                    kind="path_unmapped",
                                    severity="warning",
                                    path=joined,
                                    detail=(
                                        f"save_path 不以 {cfg.downloader_prefix} "
                                        "开头，无法映射到本容器路径"
                                    ),
                                )
                            )
                            continue
                        size = await asyncio.to_thread(
                            _safe_size, local
                        )
                        if size is None:
                            finding = Finding(
                                kind="qb_file_missing",
                                severity="error",
                                path=local,
                                detail=f"qB 记录 {f['size']} 字节，磁盘上找不到",
                                torrent_hash=task["hash"],
                                fixable=cfg.auto_fix and cfg.fix_recheck,
                            )
                            elsewhere = await asyncio.to_thread(
                                _find_elsewhere, cfg.search_roots, f["name"]
                            )
                            if elsewhere:
                                finding.found_elsewhere = elsewhere
                                # 文件只是被挪走了：recheck 会重新下载一份，
                                # 先别动手，报告出来让人决定
                                finding.fixable = False
                                finding.severity = "warning"
                            findings.append(finding)
                            if finding.fixable:
                                recheck_targets.setdefault(task["hash"], []).append(
                                    finding
                                )
                        elif size != f["size"]:
                            findings.append(
                                Finding(
                                    kind="qb_size_mismatch",
                                    severity="error",
                                    path=local,
                                    detail=f"磁盘 {size} 字节，qB 记录 {f['size']}",
                                    torrent_hash=task["hash"],
                                    fixable=cfg.auto_fix and cfg.fix_recheck,
                                )
                            )
                            recheck_targets.setdefault(task["hash"], []).append(
                                findings[-1]
                            )
                        else:
                            qb_ok += 1
            except Exception as e:
                errors.append(f"查询 qB 失败：{type(e).__name__}: {e}")

        # --- AB 侧 -------------------------------------------------------
        ab_rows: list[dict[str, Any]] = []
        try:
            ab_rows = await self._read_ab_rows()
        except Exception as e:
            errors.append(f"读取 AB 数据库失败：{type(e).__name__}: {e}")

        by_bangumi: dict[int, dict[str, Any]] = {}
        skipped_no_episode = 0
        for r in ab_rows:
            if r.get("deleted"):
                continue
            ep = episode_of(r.get("torrent_name"))
            if ep is None:
                skipped_no_episode += 1
                continue
            slot = by_bangumi.setdefault(
                r["bangumi_id"],
                {
                    "title": r.get("title"),
                    "save_path": r.get("save_path"),
                    "archived": bool(r.get("archived")),
                    "eps": {},
                },
            )
            slot["eps"].setdefault(ep, r)

        ab_checked = 0
        for bangumi_id, slot in by_bangumi.items():
            save_path = slot["save_path"] or ""
            local_dir = self.to_local(save_path)
            if local_dir is None:
                findings.append(
                    Finding(
                        kind="path_unmapped",
                        severity="warning",
                        path=save_path,
                        detail=f"番剧 save_path 不以 {cfg.downloader_prefix} 开头",
                        bangumi=slot["title"],
                        bangumi_id=bangumi_id,
                    )
                )
                continue

            exists = await asyncio.to_thread(os.path.isdir, local_dir)
            if not exists:
                findings.append(
                    Finding(
                        kind="ab_dir_missing",
                        severity="error",
                        path=local_dir,
                        detail="番剧目录不存在，多半是路径映射配错了",
                        bangumi=slot["title"],
                        bangumi_id=bangumi_id,
                        archived=slot["archived"],
                    )
                )
                continue

            names = await asyncio.to_thread(os.listdir, local_dir)
            disk_eps: set[int] = set()
            for n in names:
                e = episode_of(n)
                if e is not None:
                    disk_eps.add(e)

            for ep, row in sorted(slot["eps"].items()):
                ab_checked += 1
                if ep in disk_eps:
                    continue
                covered = ep in qb_eps.get(bangumi_id, set())
                findings.append(
                    Finding(
                        kind="ab_file_missing",
                        severity="error",
                        path=f"{local_dir} · 第 {ep} 集",
                        detail=(
                            "AB 标记已下载，但目录里没有这一集；"
                            + ("qB 里还有对应任务" if covered else "qB 里也没有对应任务")
                        ),
                        bangumi=slot["title"],
                        episode=ep,
                        bangumi_id=bangumi_id,
                        archived=slot["archived"],
                        torrent_url=row.get("url"),
                        save_path=save_path,
                        fixable=(
                            cfg.auto_fix
                            and cfg.fix_redispatch
                            and not covered
                            and bool(row.get("url"))
                            # 已归档的番剧通常是有意清理过的，只报告不补
                            and not slot["archived"]
                        ),
                    )
                )

        # --- 自动修复 -----------------------------------------------------
        fixed = 0
        attempted = 0
        if cfg.auto_fix and root_ok and (recheck_targets or any(
            f.fixable for f in findings
        )):
            try:
                host, user, password = await asyncio.to_thread(self._qb_conn)
                async with QBClient(host, user, password, cfg.qb_timeout) as qb:
                    # 1) 文件缺失/尺寸不符 -> recheck，让 qB 自己补
                    if cfg.fix_recheck:
                        for h, affected in recheck_targets.items():
                            if attempted >= cfg.max_fix_per_run:
                                break
                            live = [f for f in affected if f.fixable and not f.fixed]
                            if not live:
                                continue
                            attempted += 1
                            code, text = await qb.post_form(
                                "/api/v2/torrents/recheck", {"hashes": h}
                            )
                            for finding in live:
                                if 200 <= code < 300:
                                    finding.fixed = True
                                    finding.fix_action = "qB recheck"
                                    fixed += 1
                                else:
                                    finding.fix_error = (
                                        f"recheck HTTP {code} {text[:80]}"
                                    )

                    # 2) AB 标记已下载但 qB 没任务 -> 重新投递种子
                    if cfg.fix_redispatch:
                        for finding in findings:
                            if attempted >= cfg.max_fix_per_run:
                                break
                            if (
                                finding.kind != "ab_file_missing"
                                or not finding.fixable
                                or finding.fixed
                            ):
                                continue
                            attempted += 1
                            code, text = await qb.post_form(
                                "/api/v2/torrents/add",
                                {
                                    "urls": finding.torrent_url or "",
                                    # 用番剧自己的保存目录，否则文件会落到下载根目录
                                    "savepath": (
                                        finding.save_path
                                        or self.config.downloader_prefix
                                    ),
                                    "category": "Bangumi",
                                    "tags": f"ab:{finding.bangumi_id}",
                                    "paused": "false",
                                    "stopped": "false",
                                    "autoTMM": "false",
                                    "contentLayout": "NoSubfolder",
                                },
                            )
                            if 200 <= code < 300:
                                finding.fixed = True
                                finding.fix_action = "重新投递种子"
                                fixed += 1
                            else:
                                finding.fix_error = (
                                    f"投递 HTTP {code} {text[:80]}"
                                )
            except Exception as e:
                errors.append(f"自动修复失败：{type(e).__name__}: {e}")

        findings.sort(key=lambda f: (f.kind, f.path))
        duration_ms = int((time.monotonic() - started) * 1000)

        summary = {
            "qb_torrents": len(qb_torrents),
            "qb_files_checked": qb_files_checked,
            "qb_files_ok": qb_ok,
            "ab_rows_downloaded": len(ab_rows),
            "ab_episodes_checked": ab_checked,
            "ab_skipped_no_episode": skipped_no_episode,
            "findings": len(findings),
            "fixed": fixed,
            "fix_attempted": attempted,
            "errors": errors,
        }
        self.ctx.log.info(
            "对账完成：qB 文件 %s/%s 正常，AB 集数 %s，问题 %s 条，修复 %s 条，耗时 %sms%s",
            qb_ok,
            qb_files_checked,
            ab_checked,
            len(findings),
            fixed,
            duration_ms,
            f"，错误 {len(errors)} 条" if errors else "",
        )
        for f in findings[:20]:
            self.ctx.log.warning(
                "[%s] %s%s", f.kind, f.path, f"（{f.detail}）" if f.detail else ""
            )

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "duration_ms": duration_ms,
            "download_root": cfg.local_prefix,
            "download_root_readable": root_ok,
            "auto_fix": cfg.auto_fix,
            "summary": summary,
            "findings": [asdict(f) for f in findings],
        }

    # ------------------------------------------------------------ 扩展点

    @provider(points.SCHEDULED_TASK, id="file-reconcile")
    def reconcile_task(self) -> ScheduledTask:
        return ScheduledTask(
            run=self._tick,
            interval=lambda: max(60.0, self.config.interval_hours * 3600),
            initial_delay=180.0,
        )

    async def _tick(self) -> None:
        await self.run_once()

    @provider(points.API_ROUTER, id="api")
    def api(self) -> APIRouter:
        router = APIRouter()

        @router.get("/report")
        async def report() -> dict[str, Any]:
            """上一轮的完整报告；还没跑过时返回空报告。"""
            stored = await self.ctx.kv.get("last_report")
            return stored or {
                "generated_at": None,
                "summary": {"findings": 0, "errors": ["尚未执行过对账"]},
                "findings": [],
            }

        @router.post("/check")
        async def check() -> dict[str, Any]:
            """立即跑一轮并返回报告。"""
            return await self.run_once()

        return router


def _safe_size(path: str) -> int | None:
    """文件不存在返回 None；存在返回字节数。"""
    try:
        return os.path.getsize(path)
    except OSError:
        return None


def _find_elsewhere(roots: list[str], filename: str) -> str | None:
    """在其它目录里按文件名找，用于报告「被移动到哪了」。"""
    for root in roots:
        if not root or not os.path.isdir(root):
            continue
        for dirpath, _dirs, files in os.walk(root):
            if filename in files:
                return os.path.join(dirpath, filename)
    return None


def _track_bangumi(
    qb_eps: dict[int, set[int]],
    task: dict[str, Any],
    files: list[dict[str, Any]] | None = None,
) -> None:
    """从 qB 任务的 tag ``ab:<id>`` 记下「这个番剧在 qB 里有哪些集」。

    集数从任务名和它的文件名两边各解析一次：多文件任务的任务名是目录名，
    不一定带集数，而文件名才是可靠来源。
    """
    bangumi_id = None
    for tag in str(task.get("tags") or "").split(","):
        tag = tag.strip()
        if tag.startswith("ab:") and tag[3:].isdigit():
            bangumi_id = int(tag[3:])
            break
    if bangumi_id is None:
        return
    bucket = qb_eps.setdefault(bangumi_id, set())
    for name in [task.get("name")] + [f.get("name") for f in (files or [])]:
        ep = episode_of(name)
        if ep is not None:
            bucket.add(ep)
