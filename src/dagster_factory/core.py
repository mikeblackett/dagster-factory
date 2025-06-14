from collections.abc import (
    Hashable,
    Iterable,
    Iterator,
    Mapping,
    Sequence,
    Set,
)
from functools import cached_property
from typing import Any, cast

import dagster as dg

from dagster_factory.types import (
    TCoercibleToAssetDep,
    TCoercibleToAssetIn,
    TCoercibleToAssetKey,
    TCoercibleToAssetKeyPrefix,
)

EMPTY_ASSET_KEY_SENTINEL = dg.AssetKey([])


class AssetMember:
    """Define an asset that is a member of an asset group.

    Attributes
    ----------
    key: AssetKey
        The unique identifier for this asset
    code_version: str | None
        The version of the code for this specific asset.
    dagster_type: DagsterType | None
        Allows specifying type validation functions that will be executed on
        the input of the decorated function before it runs.
    deps: Iterable[AssetDep]
        The asset keys for the upstream assets that this asset depends on.
    description: str | None
        Human-readable description of this asset.
    group_name: str | None
        A string name used to organize multiple assets into groups. If not
        provided, the name "default" is used.
    ins: Mapping[str, AssetIn]
        A dictionary that maps input names to information about the input.
    io_manager_key: str | None
        The resource key of the IOManager used for storing the output of the
        op as an asset, and for loading it in downstream ops. If not provided,
        the default IOManager is used.
    key_prefix: Sequence[str]
        The prefix of the asset key. This is all elements of the asset key
        except the last one.
    kinds: Set[str]
        A list of strings representing the kinds of the asset. These will be
        made visible in the Dagster UI.
    metadata: Mapping[str, Any]
        A dictionary of metadata entries for the asset.
    name: str
        The name of the asset. This is the last element of the asset key.
    owners: Sequence[str] | None
        A list of strings representing owners of the asset.
    partitions_def: PartitionsDefinition | None
        Defines the set of partition keys that compose the asset.
    skippable: bool
        Whether this asset can be omitted during materialization, causing
        downstream dependencies to skip.
    tags: Mapping[str, str]
        A dictionary of tags for filtering and organizing. These tags are not
        attached to runs of the asset.
    """

    def __init__(
        self,
        key: TCoercibleToAssetKey,
        *,
        code_version: str | None = None,
        dagster_type: dg.DagsterType | None = None,
        deps: Iterable[TCoercibleToAssetDep] | None = None,
        description: str | None = None,
        group_name: str | None = None,
        ins: Mapping[str, TCoercibleToAssetIn] | None = None,
        io_manager_key: str | None = None,
        kinds: Set[str] | None = None,
        metadata: Mapping[str, Any] | None = None,
        owners: Sequence[str] | None = None,
        partitions_def: dg.PartitionsDefinition | None = None,
        skippable: bool = False,
        tags: Mapping[str, str] | None = None,
    ):
        """Build an AssetMember from the provided parameters.

        Parameters
        ----------
        key: AssetKey
            The unique identifier for this asset.
        code_version: str | None
            The version of the code for this specific asset.
        dagster_type: DagsterType | None
            Allows specifying type validation functions that will be executed on
            the input of the decorated function before it runs.
        deps: Iterable[AssetDep]
            The asset keys for the upstream assets that this asset depends on.
        description: str | None
            Human-readable description of this asset.
        group_name: str | None
            A string name used to organize multiple assets into groups. If not
            provided, the name "default" is used.
        ins: Mapping[str, AssetIn]
            A dictionary that maps input names to information about the input.
        io_manager_key: str | None
            The resource key of the IOManager used for storing the output of the
            op as an asset, and for loading it in downstream ops. If not provided,
            the default IOManager is used.
        kinds: Set[str]
            A list of strings representing the kinds of the asset. These will be
            made visible in the Dagster UI.
        metadata: Mapping[str, Any]
            A dictionary of metadata entries for the asset.
        owners: Sequence[str] | None
            A list of strings representing owners of the asset.
        partitions_def: PartitionsDefinition | None
            Defines the set of partition keys that compose the asset.
        skippable: bool
            Whether this asset can be omitted during materialization, causing
            downstream dependencies to skip.
        tags: Mapping[str, str]
            A dictionary of tags for filtering and organizing. These tags are
            not attached to runs of the asset.

        Returns
        -------
        AssetMember
            The asset member built from the parameters.
        """
        self._spec = dg.AssetSpec(
            code_version=code_version,
            deps=deps,
            description=description,
            group_name=group_name,
            key=key,
            kinds=set(kinds or []),
            metadata=metadata,
            owners=owners,
            partitions_def=partitions_def,
            skippable=skippable,
            tags=tags,
        )
        self.ins = _coerce_to_ins(ins)
        self.dagster_type = dagster_type
        self.io_manager_key = io_manager_key

    def __getattr__(self, name: str) -> Any:
        # Delegate attribute access to the underlying ``AssetSpec``
        try:
            return getattr(self._spec, name)
        except AttributeError as error:
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{name}'"
            ) from error

    @property
    def key_prefix(self) -> Sequence[str]:
        """The prefix of the asset key."""
        return self.key.path[:-1]

    @property
    def name(self) -> str:
        """The name of the asset."""
        return self.key.path[-1]

    def to_spec(
        self,
        key: TCoercibleToAssetKey | None = None,
        deps: Sequence[dg.AssetDep] | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> dg.AssetSpec:
        """Construct an :py:class:`~dagster.AssetSpec` from this ``AssetMember``.

        Parameters
        ----------
        key: AssetKey | None
            The key of the asset. This will override the component's key.
        deps: Sequence[AssetDep] | None
            The dependencies of the asset. This will augment the component's
            dependencies.
        tags: Mapping[str, str] | None
            The tags of the asset. This will augment the component's tags.
            If a tag is present in both the component and the passed tags,
            the passed tag will override the component's tag.

        Returns
        -------
        AssetSpec
            The asset specification built from the component.
        """
        return self._spec.replace_attributes(
            key=key or self.key,
            deps=[*self.deps, *(deps or [])],
            tags={**self.tags, **(tags or {})},
        )

    def to_in(
        self,
        key_prefix: Sequence[str] | None = None,
        metadata: Mapping[str, Any] | None = None,
        partition_mapping: dg.PartitionMapping | None = None,
    ) -> dg.AssetIn:
        """Construct an :py:class:`~dagster.AssetIn` from this ``AssetMember``.

        Parameters
        ----------
        key_prefix: Sequence[str] | None
            The prefix of the asset key. The key will be the concatenation of
            the key prefix and the component's key.
        metadata: Mapping[str, Any] | None
            A dictionary of metadata entries for the asset in.
        partition_mapping: PartitionMapping | None
            Defines what partitions to depend on in the upstream asset. If not
            provided, defaults to the default partition mapping for the
            partitions definition.

        Returns
        -------
        AssetIn
            The asset input built from the component.
        """
        return dg.AssetIn(
            key=self.key.with_prefix(key_prefix or []),
            metadata=metadata,
            partition_mapping=partition_mapping,
            dagster_type=self.dagster_type,  # type: ignore
        )

    def to_dep(
        self,
        partition_mapping: dg.PartitionMapping | None = None,
    ) -> dg.AssetDep:
        """Construct an :py:class:`~dagster.AssetDep` from this ``AssetMember``.

        Parameters
        ----------
        partition_mapping: PartitionMapping | None
            Defines what partitions to depend on in the upstream asset. If not
            provided, defaults to the default partition mapping for the
            partitions definition.

        Returns
        -------
        AssetDep
            The asset dep built from the component.
        """
        return dg.AssetDep(
            asset=self.key,
            partition_mapping=partition_mapping,
        )

    def replace_attributes(
        self,
        *,
        code_version: str | None = None,
        dagster_type: dg.DagsterType | None = None,
        deps: Iterable[TCoercibleToAssetDep] | None = None,
        description: str | None = None,
        group_name: str | None = None,
        ins: Mapping[str, TCoercibleToAssetIn] | None = None,
        io_manager_key: str | None = None,
        key: TCoercibleToAssetKey | None = None,
        kinds: Set[str] | None = None,
        metadata: Mapping[str, Any] | None = None,
        owners: Sequence[str] | None = None,
        partitions_def: dg.PartitionsDefinition[str] | None = None,
        skippable: bool = False,
        tags: Mapping[str, str] | None = None,
    ):
        """Return a new ``AssetMember`` with the specified attributes replaced."""
        return type(self)(
            code_version=code_version or self.code_version,
            dagster_type=dagster_type or self.dagster_type,
            deps=deps or self.deps or [],
            description=description or self.description,
            group_name=group_name or self.group_name,
            ins=ins or self.ins,
            io_manager_key=io_manager_key or self.io_manager_key,
            key=key or self.key,
            kinds=kinds or self.kinds,
            metadata=metadata or self.metadata or {},
            owners=owners or self.owners,
            partitions_def=partitions_def or self.partitions_def,
            skippable=skippable or self.skippable,
            tags=tags or self.tags,
        )

    def merge_attributes(
        self,
        *,
        deps: Iterable[TCoercibleToAssetDep] | None = None,
        ins: Mapping[str, TCoercibleToAssetIn] | None = None,
        kinds: set[str] | None = None,
        metadata: Mapping[str, Any] | None = None,
        tags: Mapping[str, str] | None = None,
    ):
        """Return a new ``AssetMember`` with the specified attributes merged."""
        return type(self)(
            code_version=self.code_version,
            dagster_type=self.dagster_type,
            deps=[*(self.deps or []), *(deps or [])],
            description=self.description,
            group_name=self.group_name,
            ins={**self.ins, **(ins or {})},
            io_manager_key=self.io_manager_key,
            key=self.key,
            kinds={*(self.kinds or []), *(kinds or [])},
            metadata={**(self.metadata or {}), **(metadata or {})},
            owners=self.owners,
            partitions_def=self.partitions_def,
            skippable=self.skippable,
            tags={**(self.tags or {}), **(tags or {})},
        )

    @classmethod
    def from_spec(
        cls,
        spec: dg.AssetSpec,
        *,
        dagster_type: dg.DagsterType | None = None,
        ins: Mapping[str, TCoercibleToAssetIn] | None = None,
        io_manager_key: str | None = None,
    ):
        """Construct a new ``AssetMember`` from an :py:class:`~dagster.AssetSpec`.

        Parameters
        ----------
        spec: AssetSpec
            The asset specification to build the component from.
        dagster_type: DagsterType | None
            The Dagster type of the asset.
        ins: Mapping[str, TCoercibleToAssetIn] | None
            A dictionary that maps input names to information about the input.
        io_manager_key: str | None
            The IO manager key for the asset.

        Returns
        -------
        AssetMember
            The asset component built from the specification.
        """
        return type(cls)(
            code_version=spec.code_version,
            dagster_type=dagster_type,
            deps=spec.deps,
            description=spec.description,
            group_name=spec.group_name,
            ins=ins,
            io_manager_key=io_manager_key,
            key=spec.key,
            kinds=spec.kinds,
            metadata=spec.metadata,
            owners=spec.owners,
            partitions_def=spec.partitions_def,
            skippable=spec.skippable,
            tags=spec.tags,
        )


class AssetGroup(Sequence):
    """
    Defines a group of asset members that share a common specification and
    materialization strategy.

    The properties defined on the asset group are shared by the asset
    members in the group. Any existing member properties are merged with
    the group properties.

    Attributes
    ----------
    code_version: str | None
        The version of the code for this asset group.
    dagster_type: DagsterType | None
        Allows specifying type validation functions that will be executed on
        the input of the decorated function before it runs.
    deps: Iterable[AssetDep]
        The asset keys for the upstream assets that this asset depends on.
    description: str | None
        Human-readable description of this asset.
    name: str
        A string name used to organize multiple assets into groups.
    ins: Mapping[str, AssetIn]
        A dictionary that maps input names to information about the input.
    io_manager_key: str | None
        The resource key of the IOManager used for storing the output of the
        op as an asset, and for loading it in downstream ops. If not provided,
        the default IOManager is used.
    key_prefix: Sequence[str]
        The prefix of the asset key. This is all elements of the asset key
        except the last one.
    kinds: Set[str]
        A set of strings representing the kinds of the asset. These will be
        made visible in the Dagster UI.
    metadata: Mapping[str, Any]
        A dictionary of metadata entries for the asset.
    owners: Sequence[str] | None
        A list of strings representing owners of the asset.
    partitions_def: PartitionsDefinition | None
        Defines the set of partition keys that compose the asset.
    skippable: bool
        Whether this asset can be omitted during materialization, causing
        downstream dependencies to skip.
    tags: Mapping[str, str]
        A dictionary of tags for filtering and organizing. These tags are not
        attached to runs of the asset.
    """

    def __init__(
        self,
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
    ):
        self._template = AssetMember(
            code_version=code_version,
            dagster_type=dagster_type,
            deps=deps,
            description=description,
            # group_name=name,
            ins=ins,
            io_manager_key=io_manager_key,
            key=EMPTY_ASSET_KEY_SENTINEL,
            kinds=set(kinds or []),
            metadata=metadata,
            owners=owners,
            partitions_def=partitions_def,
            tags=tags,
        )
        self._members = list(members or [])
        self.key_prefix = _key_prefix_from_coercible(key_prefix)

    def __getattr__(self, name: str) -> Any:
        try:
            # Delegate attribute access to the member
            return getattr(self._template, name)
        except AttributeError as error:
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{name}'"
            ) from error

    def __iter__(self):
        return iter(self.members)

    def __getitem__(  # type: ignore[override]
        self, index: int | slice
    ) -> AssetMember | Sequence[AssetMember]:
        return self.members[index]

    def __len__(self):
        return len(self.members)

    @cached_property
    def members(self) -> Sequence[AssetMember]:
        """The asset members in this group."""
        return [
            member.replace_attributes(
                key=member.key.with_prefix(self.key_prefix),
                code_version=self.code_version,
                dagster_type=self.dagster_type,
                # group_name=self.group_name,
                io_manager_key=self.io_manager_key,
                owners=self.owners,
                partitions_def=self.partitions_def,
                skippable=self.skippable,
            ).merge_attributes(
                deps=self.deps,
                ins=self.ins,
                metadata=self.metadata,
                tags=self.tags,
                kinds=set(self.kinds),
            )
            for member in self._members
        ]

    @cached_property
    def specs(self) -> Sequence[dg.AssetSpec]:
        """The asset specifications for the assets in this group."""
        return [member.to_spec() for member in self.members]

    @cached_property
    def members_by_output_name(self) -> Mapping[str, AssetMember]:
        """Return a mapping of output names to asset members."""
        return {member.name: member for member in self.members}

    def member_for_output_name(self, output_name: str) -> AssetMember:
        """Return the asset member for the given output name."""
        members = self.members_by_output_name
        if output_name in members:
            return members[output_name]
        raise ValueError(f"No member found for output name '{output_name}'.")

    def to_assets_def(self, can_subset: bool = False) -> dg.AssetsDefinition:
        """Construct an :py:class:`~dagster.AssetsDefinition` from this ``AssetGroup``."""
        specs = (
            [
                spec.with_io_manager_key(self.io_manager_key)
                for spec in self.specs
            ]
            if self.io_manager_key
            else self.specs
        )
        return dg.AssetsDefinition(specs=specs, can_subset=can_subset)

    def filter_members(
        self,
        filters: Mapping[str, Any] | None = None,
        **filters_kwargs: Any,
    ) -> Iterator[AssetMember]:
        """Return an iterator over the asset members that match the given filters.

        Parameters
        ----------
        filters : Mapping[str, Any] | None
            The filters to apply to the asset members.
        **filters_kwargs : Any
            The keyword arguments form of ``filters``.

        Returns
        -------
        Iterator
            Iterator over the asset members that match the given filters.

        """
        kwargs = _mapping_or_kwargs(
            filters, filters_kwargs, func_name='filter_members'
        )
        for t in self.members:
            if all(getattr(t, str(k)) == v for k, v in kwargs.items()):
                yield t


def _coerce_to_ins(
    coercible_to_asset_ins: Mapping[str, TCoercibleToAssetIn] | None = None,
) -> Mapping[str, dg.AssetIn]:
    """Coerce a mapping of input names to AssetIn objects."""
    if not coercible_to_asset_ins:
        return {}
    ins_set = {}
    for input_name, in_ in coercible_to_asset_ins.items():
        asset_in = _asset_in_from_coercible(in_)
        if input_name in ins_set and asset_in != ins_set[asset_in.key]:
            raise dg.DagsterInvariantViolationError(
                f'Cannot set a dependency on asset {asset_in.key} '
                'more than once per asset.'
            )
        ins_set[input_name] = asset_in

    return ins_set


def _asset_in_from_coercible(arg: TCoercibleToAssetIn) -> dg.AssetIn:
    """Coerce a value to an AssetIn object."""
    match arg:
        case dg.AssetIn():
            return arg
        case dg.AssetKey():
            return dg.AssetIn(key=arg)
        case str() | list():
            return dg.AssetIn(key=dg.AssetKey.from_coercible(arg))
        case dg.AssetSpec():
            return dg.AssetIn(
                key=arg.key,
                metadata=arg.metadata,
            )
        case _:
            raise TypeError(f'Invalid type for asset in: {arg}')


def _key_prefix_from_coercible(
    coercible_to_key_prefix: TCoercibleToAssetKeyPrefix | None,
) -> Sequence[str] | None:
    """Coerce a value to a key prefix."""
    if coercible_to_key_prefix is None:
        return None
    if isinstance(coercible_to_key_prefix, str):
        return coercible_to_key_prefix.split('/')
    return coercible_to_key_prefix


def _mapping_or_kwargs[T](
    parg: Mapping[Any, T] | None,
    kwargs: Mapping[str, T],
    func_name: str,
) -> Mapping[Hashable, T]:
    """Return a mapping of arguments from either positional or keyword arguments."""
    if parg is None or parg == {}:
        return cast(Mapping[Hashable, T], kwargs)
    if kwargs:
        raise ValueError(
            f'cannot specify both keyword and positional arguments to {func_name}'
        )
    return parg
