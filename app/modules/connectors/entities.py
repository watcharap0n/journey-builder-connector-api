"""Generic dataset types accepted by upload and connector intake.

CUSTOMER is the current customer_profile contract. TRANSACTION and EVENT are the generic
fact contracts behind Customer 360; ASSET and RELATIONSHIP carry people-to-things and
people-to-people links (households, shared vehicles). A MULTI dataset uses a mapping v2
whose projections emit several of these types from one source row.
The field contracts live in the vendored `intake_mapping` package so the API, the upload
landing and the connector worker validate the same way. Design:
buzz-cdp-workspace/ops/design/intake-v2-pipeline-2026-10-21.md
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Literal

from app.modules.connectors.intake_mapping.contracts import (
    ENTITY_FIELDS,
    ENTITY_KEY,
    ENTITY_REQUIRED,
)

TargetEntity = Literal["CUSTOMER", "TRANSACTION", "EVENT", "ASSET", "RELATIONSHIP", "MULTI"]
SINGLE_ENTITIES: tuple[str, ...] = tuple(ENTITY_FIELDS)
TARGET_ENTITIES: tuple[str, ...] = (*SINGLE_ENTITIES, "MULTI")

CUSTOMER_TARGET_FIELDS = ENTITY_FIELDS["CUSTOMER"]
TRANSACTION_TARGET_FIELDS = ENTITY_FIELDS["TRANSACTION"]
EVENT_TARGET_FIELDS = ENTITY_FIELDS["EVENT"]
ENTITY_KEY_FIELD = ENTITY_KEY
ALL_TARGET_FIELDS = frozenset().union(*ENTITY_FIELDS.values())


def mapping_errors(
    entity: str, targets: Iterable[str], *, constant_targets: Iterable[str] = ()
) -> list[str]:
    """Problems of a single-type (v1) mapping against one entity contract."""
    if entity not in ENTITY_FIELDS:
        return [f"unsupported target_entity: {entity}"]
    targets = set(targets)
    errors: list[str] = []
    unsupported = sorted(targets - ENTITY_FIELDS[entity])
    if unsupported:
        errors.append(f"unsupported {entity.lower()} target_field: " + ", ".join(unsupported))
    for group in ENTITY_REQUIRED[entity]:
        if not targets.intersection(group):
            errors.append(f"{entity.lower()} mapping requires target_field " + " or ".join(group))
    key = ENTITY_KEY_FIELD[entity]
    if key in set(constant_targets):
        errors.append(f"{key} must come from a source column, not a constant")
    if entity == "CUSTOMER" and set(constant_targets):
        # The customer worker path does not read constants.
        errors.append("customer mappings do not support constant values")
    return errors
