import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
import yaml


def _parse_list(value: str) -> List[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _load_channels_from_yaml(yaml_path: str = "channels.yaml") -> Optional[List[str]]:
    """Load channels from YAML config file."""
    path = Path(yaml_path)
    if not path.exists():
        return None
    
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
            if data and 'channels' in data:
                return [ch['username'] for ch in data['channels'] if 'username' in ch]
    except Exception as e:
        print(f"Warning: Could not load channels from {yaml_path}: {e}")
    
    return None


@dataclass
class Settings:
    api_id: int
    api_hash: str
    phone: str
    session_name: str
    channels: List[str]
    keywords: List[str]
    save_all: bool
    db_path: str
    backfill_limit: int


def load_settings() -> Settings:
    api_id = os.getenv("TG_API_ID")
    api_hash = os.getenv("TG_API_HASH")
    phone = os.getenv("TG_PHONE")
    
    # Попытаемся загрузить каналы из YAML, если там есть
    channels = _load_channels_from_yaml("channels.yaml")
    
    # Если YAML не найден или пуст, используем переменную окружения
    if not channels:
        channels_raw = os.getenv("TG_CHANNELS", "")
        channels = _parse_list(channels_raw)
    
    keywords_raw = os.getenv(
        "JOB_KEYWORDS",
        "vacancy,job,hiring,full-time,part-time,remote,офер,вакансия,ищем,работа",
    )

    if not api_id or not api_hash or not phone:
        raise RuntimeError(
            "Set TG_API_ID, TG_API_HASH, TG_PHONE in environment variables."
        )

    if not channels:
        raise RuntimeError(
            "Set TG_CHANNELS env var or add channels to channels.yaml"
        )

    save_all = os.getenv("SAVE_ALL", "1").strip().lower() in (
        "1",
        "true",
        "yes",
    )

    return Settings(
        api_id=int(api_id),
        api_hash=api_hash,
        phone=phone,
        session_name=os.getenv("TG_SESSION_NAME", "jobs_collector"),
        channels=channels,
        keywords=[k.lower() for k in _parse_list(keywords_raw)],
        save_all=save_all,
        db_path=os.getenv("DB_PATH", "jobs.db"),
        backfill_limit=int(os.getenv("BACKFILL_LIMIT", "50")),
    )
