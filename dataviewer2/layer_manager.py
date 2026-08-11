from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable


@dataclass(slots=True)
class LayerGroup:
    name: str
    layers: list[str] = field(default_factory=list)


class LayerManager:
    """Model-side source of truth for layer grouping and order.

    The Qt tree is a view only. Drag/drop changes this model first and the tree
    is rebuilt from it, avoiding unsafe QTreeWidgetItem re-parenting.
    """

    def __init__(
        self,
        group_order: Iterable[str] | None = None,
        layer_order: dict[str, list[str]] | None = None,
    ) -> None:
        self._groups: list[LayerGroup] = []
        saved = dict(layer_order or {})
        for name in group_order or []:
            key = str(name)
            self._groups.append(LayerGroup(key, [str(v) for v in saved.get(key, [])]))
        for name, layers in saved.items():
            if not self.has_group(str(name)):
                self._groups.append(LayerGroup(str(name), [str(v) for v in layers]))

    @property
    def groups(self) -> tuple[LayerGroup, ...]:
        return tuple(self._groups)

    def has_group(self, name: str) -> bool:
        return any(group.name == name for group in self._groups)

    def ensure_group(self, name: str, index: int | None = None) -> LayerGroup:
        name = str(name)
        for group in self._groups:
            if group.name == name:
                return group
        group = LayerGroup(name)
        if index is None:
            self._groups.append(group)
        else:
            self._groups.insert(max(0, min(int(index), len(self._groups))), group)
        return group

    def group_for_layer(self, layer_name: str) -> str | None:
        for group in self._groups:
            if layer_name in group.layers:
                return group.name
        return None

    def add_layer(self, group_name: str, layer_name: str, index: int | None = None) -> None:
        self.remove_layer(layer_name)
        group = self.ensure_group(group_name)
        if index is None:
            group.layers.append(layer_name)
        else:
            group.layers.insert(max(0, min(int(index), len(group.layers))), layer_name)

    def remove_layer(self, layer_name: str) -> None:
        for group in self._groups:
            while layer_name in group.layers:
                group.layers.remove(layer_name)

    def move_layers(self, layer_names: Iterable[str], target_group: str, index: int | None = None) -> None:
        names = []
        seen: set[str] = set()
        for raw in layer_names:
            name = str(raw)
            if name and name not in seen:
                names.append(name)
                seen.add(name)
        if not names:
            return

        target = self.ensure_group(target_group)
        # If moving inside the same group, correct the insertion point for rows
        # removed before it.
        if index is None:
            insert_at = len(target.layers)
        else:
            insert_at = max(0, min(int(index), len(target.layers)))
            insert_at -= sum(1 for name in names if name in target.layers and target.layers.index(name) < insert_at)

        for name in names:
            self.remove_layer(name)
        target = self.ensure_group(target_group)
        insert_at = max(0, min(insert_at, len(target.layers)))
        for offset, name in enumerate(names):
            target.layers.insert(insert_at + offset, name)

    def delete_group(self, group_name: str, fallback: str = "Ungrouped") -> None:
        group = next((g for g in self._groups if g.name == group_name), None)
        if group is None:
            return
        layers = list(group.layers)
        self._groups.remove(group)
        if layers:
            target = self.ensure_group(fallback)
            for name in layers:
                if name not in target.layers:
                    target.layers.append(name)

    def reorder_groups(self, names: Iterable[str]) -> None:
        order = [str(value) for value in names]
        lookup = {group.name: group for group in self._groups}
        reordered = [lookup[name] for name in order if name in lookup]
        reordered.extend(group for group in self._groups if group.name not in order)
        self._groups = reordered

    def sync(self, group_order: list[str], layer_order: dict[str, list[str]]) -> None:
        self._groups = []
        for group_name in group_order:
            self._groups.append(LayerGroup(str(group_name), list(layer_order.get(group_name, []))))

    def flattened_layers(self) -> list[str]:
        return [name for group in self._groups for name in group.layers]

    def serialize(self) -> tuple[list[str], dict[str, list[str]]]:
        return (
            [group.name for group in self._groups],
            {group.name: list(group.layers) for group in self._groups},
        )
