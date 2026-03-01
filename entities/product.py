"""Product entity: unified content dimension (YouTube videos + LinkedIn posts).

Uses UnionSource to combine two content sources with field_mappings for
column normalization and literal_columns for product_type/source_platform tags.
"""

from fyrnheim import (
    ComputedColumn,
    DimensionLayer,
    Entity,
    Field,
    LayersConfig,
    PrepLayer,
    TableSource,
    UnionSource,
)

entity = Entity(
    name="product",
    description="Unified product dimension combining YouTube videos and LinkedIn posts",
    source=UnionSource(
        sources=[
            TableSource(
                project="deepskydata",
                dataset="timodata_sources",
                table="youtube_videos",
                duckdb_path="youtube_videos/*.parquet",
                field_mappings={
                    "video_id": "product_id",
                },
                literal_columns={
                    "product_type": "video",
                    "source_platform": "youtube",
                },
            ),
            TableSource(
                project="deepskydata",
                dataset="timodata_sources",
                table="authoredup_posts",
                duckdb_path="authoredup_posts/*.parquet",
                field_mappings={
                    "post_id": "product_id",
                    "text": "title",
                    "impressions": "view_count",
                    "reactions": "like_count",
                    "comments": "comment_count",
                    "shares": "share_count",
                },
                literal_columns={
                    "product_type": "post",
                    "source_platform": "linkedin",
                },
            ),
        ],
    ),
    layers=LayersConfig(
        prep=PrepLayer(model_name="prep_product"),
        dimension=DimensionLayer(model_name="dim_product"),
    ),
)
