WITH stg_track AS (
    SELECT
        t.trackid,
        t.albumid,
        t.mediatypeid,
        t.genreid,
        t.milliseconds,
        t.bytes
    FROM {{ source('chinook', 'Track') }} t
),

stg_album AS (
    SELECT
        a.albumid,
        a.title AS albumname,
        a.artistid
    FROM {{ source('chinook', 'Album') }} a
),

stg_mediatype AS (
    SELECT
        m.mediatypeid,
        m.Name AS mediatypename
    FROM {{ source('chinook', 'mediatype') }} m
),

stg_artist AS (
    SELECT
        ar.artistid,
        ar.Name AS artistname
    FROM {{ source('chinook', 'Artist') }} ar
),

album_track_aggregate AS (
    SELECT
        tr.albumid,
        COUNT(tr.trackid) AS totaltracks,
        AVG(tr.milliseconds) AS AverageTrackLength,
        SUM(tr.bytes) AS totalstorage
    FROM stg_track tr
    GROUP BY tr.albumid
)

SELECT
    tr.trackid AS trackkey,
    a.albumid AS albumkey,
    mt.mediatypeid AS mediatypekey,
    a.artistid,
    a.albumname,
    ata.totaltracks,
    ata.AverageTrackLength,
    ata.totalstorage
FROM stg_track tr
JOIN album_track_aggregate ata ON tr.albumid = ata.albumid
JOIN stg_album a ON tr.albumid = a.albumid
JOIN stg_mediatype mt ON tr.mediatypeid = mt.mediatypeid
JOIN stg_artist ar ON a.artistid = ar.artistid