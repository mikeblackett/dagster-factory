from collections.abc import Callable, Iterable
from functools import wraps
from typing import Any, Mapping, Sequence

import dagster as dg

from dagster_factory.core import AssetGroup, AssetMember
from dagster_factory.types import TCoercibleToAssetDep, TCoercibleToAssetIn


def asset_group[T](
    name: str,
    *,
    code_version: str | None = None,
    dagster_type: dg.DagsterType | None = None,
    deps: Iterable[TCoercibleToAssetDep] | None = None,
    description: str | None = None,
    ins: Mapping[str, TCoercibleToAssetIn] | None = None,
    io_manager_key: str | None = None,
    key_prefix: str | Sequence[str] | None = None,
    kinds: Iterable[str] | None = None,
    metadata: Mapping[str, Any] | None = None,
    partitions_def: dg.PartitionsDefinition[str] | None = None,
    members: Iterable[AssetMember] | None = None,
    tags: Mapping[str, str] | None = None,
    owners: Sequence[str] | None = None,
) -> Callable[..., list[dg.AssetsDefinition]]:
    """Create a definition for how to compute a group of assets that share a
    common specification and materialization strategy.

    Parameters
    ----------


    Returns
    -------
    Callable[..., list[AssetsDefinition]]
        A decorator that can be used to decorate the compute function for the
        assets.

    Raises
    ------
    ValueError
        If no blueprints are provided.
    """

    group = AssetGroup(
        code_version=code_version,
        dagster_type=dagster_type,
        deps=deps,
        description=description,
        ins=ins,
        io_manager_key=io_manager_key,
        key_prefix=key_prefix,
        kinds=kinds,
        members=members,
        metadata=metadata,
        name=name,
        owners=owners,
        partitions_def=partitions_def,
        tags=tags,
    )

    def wrapper(
        compute_fn: Callable[..., dg.Output[T]],
    ) -> list[dg.AssetsDefinition]:
        asset_defs: list[dg.AssetsDefinition] = []

        for member in group.members:

            def make_asset(member):
                @dg.asset(
                    code_version=code_version or member.code_version,
                    dagster_type=member.dagster_type,
                    description=member.description,
                    deps=member.deps,
                    group_name=member.group_name,
                    ins=member.ins,
                    io_manager_key=member.io_manager_key,
                    key=member.key,
                    # kinds=member.kinds,
                    metadata=member.metadata,
                    partitions_def=member.partitions_def,
                    # tags=member.tags,
                )
                @wraps(compute_fn)
                def _asset(*args, **kwargs):
                    return compute_fn(*args, **kwargs)

                return _asset

            asset_defs.append(make_asset(member))

        return asset_defs

    return wrapper
