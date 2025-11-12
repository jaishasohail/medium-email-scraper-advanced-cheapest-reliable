import json
from pathlib import Path
from typing import Any, Dict

def load_settings(path: Path) -> Dict[str, Any]:
    """
    Load settings JSON. If the file doesn't exist, return sensible defaults.
    """
    defaults: Dict[str, Any] = {
        "REQUEST_TIMEOUT": 15,
        "CONCURRENCY": 2,
        "LOG_LEVEL": "INFO",
        "USER_AGENT": "Mozilla/5.0",
    }
    if not path.exists():
        return defaults
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    defaults.update(data or {})
    return defaults