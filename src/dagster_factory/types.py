from collections.abc import Sequence
import dagster as dg

type TCoercibleToAssetKey = dg.AssetKey | str | Sequence[str]
type TCoercibleToAssetKeyPrefix = str | Sequence[str]
type TCoercibleToAssetDep = (
    TCoercibleToAssetKey | dg.AssetSpec | dg.AssetsDefinition | dg.AssetDep
)
type TCoercibleToAssetIn = (
    TCoercibleToAssetKey | dg.AssetSpec | dg.AssetsDefinition | dg.AssetIn
)
type CoercibleToAssetOut = (
    TCoercibleToAssetKey | dg.AssetSpec | dg.AssetsDefinition | dg.AssetOut
)
