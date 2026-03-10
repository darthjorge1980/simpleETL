"""Base classes for all ETL plugins.

To create a new plugin, subclass one of:
  - SourcePlugin   (reads data)
  - TransformPlugin(transforms data)
  - DestinationPlugin(writes data)

and implement the required methods. Register it with the @register decorator.
"""

from __future__ import annotations

import abc
from typing import Any

import polars as pl

from core.models import ParamDefinition, PluginCategory, PluginInfo, PortDefinition, PortType

# Global plugin registry – populated by @register
_PLUGIN_REGISTRY: dict[str, type[BasePlugin]] = {}


def register(cls: type[BasePlugin]) -> type[BasePlugin]:
    """Class decorator that registers a plugin."""
    info = cls.info()
    _PLUGIN_REGISTRY[info.id] = cls
    return cls


def get_registry() -> dict[str, type[BasePlugin]]:
    return _PLUGIN_REGISTRY


class BasePlugin(abc.ABC):
    """Common interface for every plugin."""

    def __init__(self, params: dict[str, Any] | None = None):
        self.params = params or {}

    @classmethod
    @abc.abstractmethod
    def info(cls) -> PluginInfo:
        ...

    @abc.abstractmethod
    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        """Run the plugin. Return mapping of output-port-name → LazyFrame."""
        ...

    @classmethod
    @abc.abstractmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        """Return (code_snippet, {output_port: variable_name})."""
        ...


class SourcePlugin(BasePlugin):
    """Plugin that produces data (no inputs)."""

    @classmethod
    def _base_outputs(cls) -> list[PortDefinition]:
        return [PortDefinition(name="output", port_type=PortType.DATAFRAME)]


class TransformPlugin(BasePlugin):
    """Plugin that transforms data (1+ inputs → 1+ outputs)."""

    @classmethod
    def _base_inputs(cls) -> list[PortDefinition]:
        return [PortDefinition(name="input", port_type=PortType.DATAFRAME)]

    @classmethod
    def _base_outputs(cls) -> list[PortDefinition]:
        return [PortDefinition(name="output", port_type=PortType.DATAFRAME)]


class DestinationPlugin(BasePlugin):
    """Plugin that writes data (1+ inputs, no outputs)."""

    @classmethod
    def _base_inputs(cls) -> list[PortDefinition]:
        return [PortDefinition(name="input", port_type=PortType.DATAFRAME)]
