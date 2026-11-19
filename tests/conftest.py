import sys
from pathlib import Path

# 让 `import file_reconcile` 在仓库根目录下可用（tests/ 会被 ab-plugin pack 排除）
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
