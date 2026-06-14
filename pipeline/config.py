"""配置管理模块"""
import json
import os
import copy
from datetime import datetime
from typing import Any, Dict


DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "default_config.json")


def load_config(config_path: str = None) -> Dict[str, Any]:
    """加载配置文件"""
    path = config_path or DEFAULT_CONFIG_PATH
    if not os.path.exists(path):
        raise FileNotFoundError(f"配置文件不存在: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_config(config: Dict[str, Any], config_path: str) -> str:
    """保存配置到文件，返回配置路径"""
    os.makedirs(os.path.dirname(config_path), exist_ok=True)
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    return config_path


def get_default_config() -> Dict[str, Any]:
    """获取默认配置的深拷贝"""
    return copy.deepcopy(load_config(DEFAULT_CONFIG_PATH))


def config_to_json(config: Dict[str, Any]) -> str:
    """将配置转为 JSON 字符串"""
    return json.dumps(config, indent=2, ensure_ascii=False)


def config_from_json(json_str: str) -> Dict[str, Any]:
    """从 JSON 字符串解析配置"""
    return json.loads(json_str)


def bump_config_version(config: Dict[str, Any]) -> Dict[str, Any]:
    """增加配置版本号"""
    config = copy.deepcopy(config)
    config["version"] = config.get("version", 0) + 1
    return config
