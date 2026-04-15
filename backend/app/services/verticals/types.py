from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass(frozen=True)
class VerticalSpec:
    """
    Pure data container for a vertical pack.

    IMPORTANT:
    - This class contains ZERO engine logic.
    - It only describes schemas + policies loaded from disk.
    - The engine (verdict/proof) consumes this data elsewhere.
    """

    # Core identity
    vertical_id: str
    name: str
    version: str

    # Loaded artifacts
    object_schema: Dict[str, Any] = field(default_factory=dict)
    events_schema: Dict[str, Any] = field(default_factory=dict)
    policies: Dict[str, Any] = field(default_factory=dict)

    # Optional UI / descriptive metadata
    labels: Dict[str, Any] = field(default_factory=dict)

    def pack_dict(self) -> Dict[str, Any]:
        """
        Safe, compact representation of the vertical pack.
        This is what UI / proof bundles / APIs may expose.
        """
        return {
            "id": self.vertical_id,
            "name": self.name,
            "version": self.version,
            "labels": self.labels or {},
            "policy": {
                "id": (self.policies or {}).get("id", ""),
            },
        }
