from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any


@dataclass
class FieldPermissions:
    read: dict[str, list[str] | str] = field(default_factory=dict)

    write: dict[str, list[str] | str] = field(default_factory=dict)

    hidden_by_default: list[str] = field(default_factory=list)

    anonymous_role: str = "guest"

    def filter_response(self, data: dict[str, Any], role: str) -> dict[str, Any]:
        result = {k: v for k, v in data.items() if k not in self.hidden_by_default}

        allowed = self.read.get(role)
        if allowed is None:
            return result
        if allowed == "__all__":
            return result

        return {k: v for k, v in result.items() if k in allowed}

    def filter_write(self, data: dict[str, Any], role: str) -> dict[str, Any]:
        allowed = self.write.get(role)
        if allowed is None:
            return {}         
        if allowed == "__all__":
            return data
        return {k: v for k, v in data.items() if k in allowed}

    def can_read_field(self, field_name: str, role: str) -> bool:
        if field_name in self.hidden_by_default:
            return False
        allowed = self.read.get(role)
        if allowed is None:
            return True         
        return allowed == "__all__" or field_name in allowed

    def can_write_field(self, field_name: str, role: str) -> bool:
        allowed = self.write.get(role)
        if allowed is None:
            return False
        return allowed == "__all__" or field_name in allowed
