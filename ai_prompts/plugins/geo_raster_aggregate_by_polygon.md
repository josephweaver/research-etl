# `geo/raster_aggregate_by_polygon`

Use for:

- aggregating raster values by polygons
- emitting polygon fields, raster summary fields, and contextual fields into one output table

Good fit:

- county or field aggregation from rasters
- polygon-level raster summaries
- mixed output rows with both polygon attributes and computed raster aggregates

Important notes:

- this plugin replaced older county-specific aggregation patterns
- use polygon-oriented argument names and output conventions
- prefer explicit `columns` specs when downstream scripts expect particular field names

Useful source forms include:

- `polygon.<field>`
- `polygon.id`
- `polygon.name`
- `raster.<agg>`
- `context.day`
- `context.raster_path`
- `literal`

Common mistakes:

- assuming older county-specific field names
- pushing source-specific output contracts into downstream scripts when the plugin can emit compatibility names directly
- forgetting that categorical/value-count style outputs may require explicit column definitions

Working rule:

- if the aggregation need is still polygon/raster standard, prefer the plugin
- if the logic becomes highly source-specific, derive a project script instead of overloading the plugin contract
