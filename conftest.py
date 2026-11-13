"""pytest 引导：-I 隔离模式下把仓库根加入 sys.path。"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
