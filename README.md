# file-reconcile

AutoBangumi 插件：**文件与下载任务对账**。

定时核对两边是否真的对得上——

| 对账关系 | 检查内容 |
| --- | --- |
| qBittorrent 任务 ↔ 磁盘 | 每个任务的每个文件是否存在、字节数是否与 qB 记录一致 |
| AutoBangumi「已下载」标记 ↔ 磁盘 | `torrent.downloaded = 1` 的记录，对应集数的文件是否还在 |

发现问题后可以自动修复，**只走 qB API**：不动 `data.db`，不写、不删任何媒体文件。

---

## 为什么需要它

AB 的 `check_new()` 按**种子 URL 是否已在 `torrent` 表里**去重，不看 `downloaded` 标记，
也不看文件是否还在。所以一旦文件从下载目录消失：

1. qB 重启时 `fast resume rejected … mismatching file size`，判定种子损坏；
2. 种子（连同记录）被清理，磁盘上永远不会有这个文件；
3. AB 仍然认为 `downloaded = 1`，`check_new` 每轮都跳过它——**永远不会重新下载**。

人工发现时往往已经过去好几天。本插件把这条链路变成每 6 小时一次的自动对账。

## 安装

**从插件市场**（需要 AutoBangumi 4.0.0-beta.2 及之后）：

设置 → 插件 → 插件市场 → 找到「文件与下载任务对账」→ 安装。

**本地安装**（开发或提前体验）：

```bash
# 把本仓库放到 AutoBangumi 的 config/plugins/local/ 下
ln -s /path/to/ab-file-reconcile /path/to/AB/config/plugins/local/file-reconcile
```

然后在 设置 → 插件 里打开「允许未签名插件」（`plugins.allow_unsigned`）并启用本插件。

## 配置

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `interval_hours` | `6` | 对账间隔（小时） |
| `downloader_prefix` | `/downloads` | qB 里 `save_path` 的开头部分 |
| `local_prefix` | `/media/我的番剧` | 同一批文件在本容器看到的路径 |
| `search_roots` | `["/media"]` | 文件不在原位时按文件名去找，用于报告「被移动到哪了」；留空则不找 |
| `auto_fix` | `true` | 是否自动修复 |
| `fix_recheck` | `true` | 文件缺失时让 qB 重新校验并补下 |
| `fix_redispatch` | `true` | AB 标记已下载但 qB 没有任务时，用记录里的种子 URL 重新投递 |
| `max_fix_per_run` | `10` | 单轮最多修复数 |
| `qb_host` / `qb_username` / `qb_password` | 空 | 留空则读取 AB 自己的下载器设置 |
| `db_path` | `/app/data/data.db` | AB 数据库路径 |
| `qb_timeout` | `30` | qB 请求超时（秒） |

**前提**：下载目录必须挂载进 AutoBangumi 容器。`local_prefix` 指向的目录读不到时，
插件会**整轮禁修**并报告，而不是把所有文件都当成缺失。

```yaml
# docker-compose.yml
volumes:
  - ./config:/app/config
  - ./data:/app/data
  - /path/to/你的下载目录:/media/你的下载目录:ro   # 只读即可
```

## 接口

需要登录（未带会话访问返回 401）：

```
GET  /api/v1/plugins/file-reconcile/report   # 上一轮的完整报告
POST /api/v1/plugins/file-reconcile/check    # 立即跑一轮并返回报告
```

报告结构：

```json
{
  "generated_at": "2026-11-19T03:26:44+00:00",
  "duration_ms": 1078,
  "download_root": "/media/我的番剧",
  "download_root_readable": true,
  "auto_fix": true,
  "summary": {
    "qb_torrents": 108,
    "qb_files_checked": 108,
    "qb_files_ok": 108,
    "ab_rows_downloaded": 188,
    "ab_episodes_checked": 109,
    "ab_skipped_no_episode": 6,
    "findings": 0,
    "fixed": 0,
    "fix_attempted": 0,
    "errors": []
  },
  "findings": []
}
```

`findings[].kind` 取值：

| kind | 含义 | 默认处理 |
| --- | --- | --- |
| `qb_file_missing` | qB 有任务，磁盘上没有这个文件 | `recheck`；若在 `search_roots` 里找到了副本则只报告 |
| `qb_size_mismatch` | 文件在，但字节数与 qB 记录不符 | `recheck` |
| `ab_file_missing` | AB 标记已下载，但目录里没有这一集 | qB 也没有对应任务时重新投递 |
| `ab_dir_missing` | 番剧目录整个不存在 | 只报告（多半是 `local_prefix` 配错） |
| `path_unmapped` | `save_path` 不以 `downloader_prefix` 开头 | 只报告 |

## 安全设计

- 媒体目录以 **`:ro`** 挂载，插件结构上无法写入或删除文件。
- 下载目录不可读 → **整轮禁修**（`test_unreadable_root_blocks_all_fixes` 覆盖）。
- 文件只是被移走（在 `search_roots` 里找得到）→ 只报告位置，**不** `recheck`，
  避免白白重下几个 GB。
- 已归档的番剧（`archived = 1`）→ 只报告不补，通常是有意清理过的。
- 单轮最多 `max_fix_per_run` 条修复。
- 读数据库用 `mode=ro` + `busy_timeout`，与宿主的写入互不阻塞（库是 WAL 模式）。

## 设计要点

**关联两边靠「番剧 → save_path → 集数」，不靠 hash。**
`torrent.qb_hash` 在实际部署里可能是空的；而且 AB 存的是 mikan RSS 标题、
qB 存的是种子内部名，两边文件名根本不同（例）：

```
AB:   [LoliHouse] 转校生美少女…/ 転校先の…/ てんびん - 09 [WebRip…]
qB:   [LoliHouse] Tenbin - 09 [WebRip…].mkv
```

所以两边各解析一次集数再比对。`episode_of()` 覆盖 `- 09 [`、`- 09v2 [`、
`E07`、`[07]`，并正确放过 `1080p` / `HEVC-10bit` 里的数字与无集数的剧场版。

## 开发

```bash
uv add ../autobangumi_sdk-0.6.0-py3-none-any.whl   # SDK 不在 PyPI，从 Release 下载
uv run ab-plugin validate .
uv run pytest
```

## License

MIT
