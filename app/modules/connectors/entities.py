"""Generic dataset types accepted by upload and connector intake.

CUSTOMER is the current customer_profile contract. TRANSACTION and EVENT are the
generic fact contracts behind Customer 360 (design:
buzz-cdp-workspace/ops/design/generic-intake-and-customer-360-2026-10-21.md).
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Literal

TargetEntity = Literal["CUSTOMER", "TRANSACTION", "EVENT"]
TARGET_ENTITIES: tuple[str, ...] = ("CUSTOMER", "TRANSACTION", "EVENT")

CUSTOMER_TARGET_FIELDS = frozenset({
    "source_customer_id",
    "external_id",
    "first_name",
    "middle_name",
    "last_name",
    "full_name",
    "nickname",
    "gender",
    "date_of_birth",
    "birthday",
    "age",
    "email",
    "phone",
    "line_user_id",
    "facebook_id",
    "google_id",
    "apple_id",
    "member_code",
    "customer_type",
    "status",
    "language",
    "address_line_1",
    "address_line_2",
    "district",
    "province",
    "postal_code",
    "country",
    "source",
    "source_system",
    "external_ids",
    "attributes",
    "source_created_at",
    "source_updated_at",
})

# One row = one transaction (purchase, refund, point earn/burn/expire, redeem, payment...).
# `transaction_type` + `currency` keep money and points apart; consumers filter them first.
TRANSACTION_TARGET_FIELDS = frozenset({
    "source_transaction_id",
    "source_customer_id",  # empty = anonymous
    "transaction_type",
    "status",
    "occurred_at",
    "amount",
    "currency",
    "discount_amount",
    "quantity",
    "channel",
    "location_id",
    "items",  # product/asset refs, not person ids
    "attributes",
    "source_updated_at",
})

# One row = one occurrence (check-in, session, view/click, survey, message outcome...).
EVENT_TARGET_FIELDS = frozenset({
    "source_event_id",
    "source_customer_id",
    "anonymous_id",
    "event_name",
    "event_category",
    "occurred_at",
    "channel",
    "object_type",
    "object_id",
    "value",
    "attributes",
})

ENTITY_FIELDS: dict[str, frozenset[str]] = {
    "CUSTOMER": CUSTOMER_TARGET_FIELDS,
    "TRANSACTION": TRANSACTION_TARGET_FIELDS,
    "EVENT": EVENT_TARGET_FIELDS,
}

# Each inner tuple is "at least one of".
ENTITY_REQUIRED: dict[str, tuple[tuple[str, ...], ...]] = {
    "CUSTOMER": (("source_customer_id",),),
    "TRANSACTION": (
        ("source_transaction_id",),
        ("transaction_type",),
        ("occurred_at",),
        ("amount",),
        ("currency",),
    ),
    "EVENT": (
        ("source_event_id",),
        ("event_name",),
        ("occurred_at",),
        ("source_customer_id", "anonymous_id"),
    ),
}

ENTITY_KEY_FIELD = {
    "CUSTOMER": "source_customer_id",
    "TRANSACTION": "source_transaction_id",
    "EVENT": "source_event_id",
}

ALL_TARGET_FIELDS = frozenset().union(*ENTITY_FIELDS.values())


def mapping_errors(
    entity: str, targets: Iterable[str], *, constant_targets: Iterable[str] = ()
) -> list[str]:
    """Return human-readable problems for a mapping against one entity contract."""
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
