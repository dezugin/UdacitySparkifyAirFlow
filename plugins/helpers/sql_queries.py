class SqlQueries:
    # CREATE TABLES

    staging_events_table_create= ("""
        CREATE TABLE IF NOT EXISTS staging_events (
                    event_id    BIGINT IDENTITY(0,1)    NOT NULL,
                    artist      VARCHAR                 NULL,
                    auth        VARCHAR                 NULL,
                    firstName   VARCHAR                 NULL,
                    gender      VARCHAR                 NULL,
                    itemInSession VARCHAR               NULL,
                    lastName    VARCHAR                 NULL,
                    length      VARCHAR                 NULL,
                    level       VARCHAR                 NULL,
                    location    VARCHAR                 NULL,
                    method      VARCHAR                 NULL,
                    page        VARCHAR                 NULL,
                    registration VARCHAR                NULL,
                    sessionId   INTEGER                 NOT NULL SORTKEY DISTKEY,
                    song        VARCHAR                 NULL,
                    status      INTEGER                 NULL,
                    ts          BIGINT                  NOT NULL,
                    userAgent   VARCHAR                 NULL,
                    userId      INTEGER                 NULL
        );
    """)

    staging_songs_table_create = ("""
        CREATE TABLE IF NOT EXISTS staging_songs (
                    num_songs           INTEGER         NULL,
                    artist_id           VARCHAR         NOT NULL SORTKEY DISTKEY,
                    artist_latitude     VARCHAR         NULL,
                    artist_longitude    VARCHAR         NULL,
                    artist_location     VARCHAR(500)   NULL,
                    artist_name         VARCHAR(500)   NULL,
                    song_id             VARCHAR         NOT NULL,
                    title               VARCHAR(500)   NULL,
                    duration            DECIMAL(9)      NULL,
                    year                INTEGER         NULL
        );
    """)

    songplay_table_create = ("""
        CREATE TABLE IF NOT EXISTS songplays (
                    songplay_id INTEGER IDENTITY(0,1)   NOT NULL SORTKEY,
                    start_time  TIMESTAMP               NOT NULL,
                    user_id     VARCHAR(50)             NOT NULL DISTKEY,
                    level       VARCHAR(10)             NOT NULL,
                    song_id     VARCHAR(40)             NOT NULL,
                    artist_id   VARCHAR(50)             NOT NULL,
                    session_id  VARCHAR(50)             NOT NULL,
                    location    VARCHAR(100)            NULL,
                    user_agent  VARCHAR(255)            NULL
        );
    """)

    user_table_create = ("""
        CREATE TABLE IF NOT EXISTS users (
                    user_id     INTEGER                 NOT NULL SORTKEY,
                    first_name  VARCHAR(50)             NULL,
                    last_name   VARCHAR(80)             NULL,
                    gender      VARCHAR(10)             NULL,
                    level       VARCHAR(10)             NULL
        ) diststyle all;
    """)

    song_table_create = ("""
        CREATE TABLE IF NOT EXISTS songs (
                    song_id     VARCHAR(50)             NOT NULL SORTKEY,
                    title       VARCHAR(500)           NOT NULL,
                    artist_id   VARCHAR(50)             NOT NULL,
                    year        INTEGER                 NOT NULL,
                    duration    DECIMAL(9)              NOT NULL
        );
    """)

    artist_table_create = ("""
        CREATE TABLE IF NOT EXISTS artists (
                    artist_id   VARCHAR(50)             NOT NULL SORTKEY,
                    name        VARCHAR(500)           NULL,
                    location    VARCHAR(500)           NULL,
                    latitude    DECIMAL(9)              NULL,
                    longitude   DECIMAL(9)              NULL
        ) diststyle all;
    """)

    time_table_create = ("""
        CREATE TABLE IF NOT EXISTS time (
                    start_time  TIMESTAMP               NOT NULL SORTKEY,
                    hour        SMALLINT                NULL,
                    day         SMALLINT                NULL,
                    week        SMALLINT                NULL,
                    month       SMALLINT                NULL,
                    year        SMALLINT                NULL,
                    weekday     SMALLINT                NULL
        ) diststyle all;
    """)

    # STAGING TABLES
    staging_template = ("""
        COPY {} 
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        json '{}'
        region 'us-west-2'
        ACCEPTINVCHARS AS '^'
        STATUPDATE ON;
    """)

    # FINAL TABLES

    songplay_table_insert = ("""
        INSERT INTO songplays (             start_time,
                                            user_id,
                                            level,
                                            song_id,
                                            artist_id,
                                            session_id,
                                            location,
                                            user_agent)
        SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
                    * INTERVAL '1 second'   AS start_time,
                se.userId                   AS user_id,
                se.level                    AS level,
                ss.song_id                  AS song_id,
                ss.artist_id                AS artist_id,
                se.sessionId                AS session_id,
                se.location                 AS location,
                se.userAgent                AS user_agent
        FROM staging_events AS se
        JOIN staging_songs AS ss
            ON (se.artist = ss.artist_name)
        WHERE se.page = 'NextSong';
    """)

    user_table_insert = ("""
        INSERT INTO users (                 user_id,
                                            first_name,
                                            last_name,
                                            gender,
                                            level)
        SELECT  DISTINCT se.userId          AS user_id,
                se.firstName                AS first_name,
                se.lastName                 AS last_name,
                se.gender                   AS gender,
                se.level                    AS level
        FROM staging_events AS se
        WHERE se.page = 'NextSong';
    """)

    song_table_insert = ("""
        INSERT INTO songs (                 song_id,
                                            title,
                                            artist_id,
                                            year,
                                            duration)
        SELECT  DISTINCT ss.song_id         AS song_id,
                ss.title                    AS title,
                ss.artist_id                AS artist_id,
                ss.year                     AS year,
                ss.duration                 AS duration
        FROM staging_songs AS ss;
    """)

    artist_table_insert = ("""
        INSERT INTO artists (               artist_id,
                                            name,
                                            location,
                                            latitude,
                                            longitude)
        SELECT  DISTINCT ss.artist_id       AS artist_id,
                ss.artist_name              AS name,
                ss.artist_location          AS location,
                ss.artist_latitude          AS latitude,
                ss.artist_longitude         AS longitude
        FROM staging_songs AS ss;
    """)

    time_table_insert = ("""
        INSERT INTO time (                  start_time,
                                            hour,
                                            day,
                                            week,
                                            month,
                                            year,
                                            weekday)
        SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
                    * INTERVAL '1 second'        AS start_time,
                EXTRACT(hour FROM start_time)    AS hour,
                EXTRACT(day FROM start_time)     AS day,
                EXTRACT(week FROM start_time)    AS week,
                EXTRACT(month FROM start_time)   AS month,
                EXTRACT(year FROM start_time)    AS year,
                EXTRACT(week FROM start_time)    AS weekday
        FROM    staging_events                   AS se
        WHERE se.page = 'NextSong';
    """)
    # Data quality verification queries:


    users_null_verify = ("""
            SELECT COUNT(*)
            FROM users
            WHERE user_id IS NULL;
        """)

    songs_null_verify = ("""
            SELECT COUNT(*)
            FROM songs
            WHERE song_id IS NULL;
        """)

    artists_null_verify = ("""
            SELECT COUNT(*)
            FROM artists
            WHERE artist_id IS NULL;
        """)

    time_null_verify = ("""
            SELECT COUNT(*)
            FROM time
            WHERE start_time IS NULL;
        """)
    songplays_null_verify = ("""
            SELECT COUNT(*)
            FROM songplays
            WHERE   songplay_id IS NULL OR
                    user_id IS NULL OR
                    start_time IS NULL;
        """)

    users_count_verify = ("""
            SELECT COUNT(*)
            FROM users;
        """)

    songs_count_verify = ("""
            SELECT COUNT(*)
            FROM songs;
        """)

    artists_count_verify = ("""
            SELECT COUNT(*)
            FROM artists;
        """)

    time_count_verify = ("""
            SELECT COUNT(*)
            FROM time;
        """)

    songplays_count_verify = ("""
            SELECT COUNT(*)
            FROM songplays;
        """)
    # verification list
    null_verify_list = [users_null_verify,songs_null_verify,artists_null_verify,time_null_verify,songplays_null_verify]
    count_verify_list = [users_count_verify,songs_count_verify,artists_count_verify,time_count_verify,songplays_count_verify]