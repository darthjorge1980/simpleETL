"""Plugin manager – discovers and loads all plugins under the plugins/ tree."""

from __future__ import annotations

import importlib
import pkgutil
from pathlib import Path

import plugins  # noqa: the package itself
from plugins.base import get_registry, BasePlugin
from core.models import PluginInfo


def _import_submodules(package_path: str, package_name: str) -> None:
    """Recursively import all submodules so @register decorators fire."""
    pkg_dir = Path(package_path)
    for importer, modname, ispkg in pkgutil.walk_packages(
        [str(pkg_dir)], prefix=package_name + "."
    ):
        importlib.import_module(modname)


def discover_plugins() -> None:
    """Import every module inside plugins/ subtree."""
    pkg = plugins
    _import_submodules(str(Path(pkg.__file__).parent), pkg.__name__)


def list_plugins() -> list[PluginInfo]:
    return [cls.info() for cls in get_registry().values()]


def get_plugin(plugin_id: str) -> type[BasePlugin] | None:
    return get_registry().get(plugin_id)
