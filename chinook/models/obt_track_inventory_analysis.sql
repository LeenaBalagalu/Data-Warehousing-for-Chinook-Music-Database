with f_trackinventoryanalysis as (
    select * from {{ ref('fact_track_inventory_analysis') }}
),
d_album as (
    select * from {{ ref('dim_album') }}
),
d_track as (
    select * from {{ ref('dim_track') }}
),
d_mediatype as (
    select * from {{ ref('dim_mediatype') }}
)
SELECT
 f.artistid,
    f.albumname,
    f.totaltracks,
    f.AverageTrackLength,
    f.totalstorage
FROM f_trackinventoryanalysis f
LEFT JOIN d_album al
    ON CAST(al.albumkey AS VARCHAR) = CAST(f.albumkey AS VARCHAR)
LEFT JOIN d_track t
    ON CAST(t.trackkey AS VARCHAR) = CAST(f.trackkey AS VARCHAR)
LEFT JOIN d_mediatype m
    ON CAST(m.mediatypekey AS VARCHAR) = CAST(f.mediatypekey AS VARCHAR)