import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os
import logging

logger = logging.getLogger(__name__)
load_dotenv()

def connect_to_db():

    try:
        conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT")
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)
    except Exception as e:
        logger.error(f"An error occurred during connection to DB: {e}")
        raise e

    return conn, cur

def disconnect_from_db(conn, cur):
    try:
        cur.close()
        conn.close()
        logger.info(f"Disconected from DB.")
    except Exception as e:
        logger.error(f"An error occurred during disconection from DB: {e}")
        raise e

def create_schema(schema):
    conn,cur = None, None
    try:
        conn, cur = connect_to_db()

        sql_query = f"CREATE SCHEMA IF NOT EXISTS {schema};"

        cur.execute(sql_query)
        conn.commit()

        disconnect_from_db(conn, cur)
        logger.info(f"Schema: {schema} created.")
    except Exception as e:
        logger.error(f"An error occurred during creating schema: {schema} Error: {e}")
        raise e

def create_tables(schema):
    conn,cur = None, None
    try:
        conn, cur = connect_to_db()

        if schema == 'raw':
            table = "raw_api_data"
            sql_query = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
            ingest_id BIGSERIAL PRIMARY KEY,
            fetched_at TIMESTAMP,
            source TEXT,
            endpoint TEXT,
            raw_json JSONB
            );
            """
            cur.execute(sql_query)
            conn.commit()

        elif schema == 'staging':
            table = "spotify_tracks_clean"
            sql_query = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                track_id TEXT,
                artist_id TEXT,
                album_id TEXT,
                artist_name TEXT,
                album_name TEXT,
                track_name TEXT,
                duration_ms INT,
                explicit BOOLEAN,
                track_number INT,
                disc_number INT,
                popularity INT,
                spotify_url TEXT,
                fetched_at TIMESTAMP
            );
            """
            cur.execute(sql_query)
            conn.commit()

            table = "itunes_tracks_clean"
            sql_query = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                itunes_track_id BIGINT,
                itunes_artist_id BIGINT,
                itunes_collection_id BIGINT,
                artist_name TEXT,
                album_name TEXT,
                track_name TEXT,
                duration_ms INT,
                track_number INT,
                disc_number INT,
                explicit TEXT,
                genre TEXT,
                release_date DATE,
                itunes_url TEXT,
                fetched_at TIMESTAMP
            );
            """
            cur.execute(sql_query)
            conn.commit()

            table = "youtube_video_clean"
            sql_query = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                youtube_video_id TEXT,
                channel_name TEXT,
                video_title TEXT,
                duration TEXT,
                published_at TIMESTAMP,
                view_count BIGINT,
                like_count BIGINT,
                comment_count BIGINT,
                video_url TEXT,
                fetched_at TIMESTAMP
            );
            """
            cur.execute(sql_query)
            conn.commit()

        elif schema == 'dim':
            table = "artists"
            sql_query = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                artist_id BIGSERIAL PRIMARY KEY,
                artist_name VARCHAR(50),
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            );
            """
            cur.execute(sql_query)
            conn.commit()

            table = "albums"
            sql_query = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                album_id BIGSERIAL PRIMARY KEY,
                album_name VARCHAR(50),
                tracks_counts INT,
                release_date TIMESTAMP,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            );
            """
            cur.execute(sql_query)
            conn.commit()

            table = "tracks"
            sql_query = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                track_id BIGSERIAL PRIMARY KEY,
                track_name VARCHAR(50),
                track_artist VARCHAR(50),
                track_album VARCHAR(50),
                release_date DATE,
                genre TEXT,
                duration TIME,
                explicit BOOLEAN,
                track_number INT,
                disc_number INT,
                popularity INT,
                spotify_url TEXT,
                itunes_url TEXT,
                view_count BIGINT,
                like_count BIGINT,
                comment_count BIGINT,
                video_url TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            );
            """
            cur.execute(sql_query)
            conn.commit()

        else:
            # fact schemas tables
            table = "track_daily_metrics"
            sql_query = f"""
            CREATE TABLE IF NOT EXISTS fact.track_daily_metrics (
                metric_id BIGSERIAL PRIMARY KEY,
                track_id BIGINT NOT NULL,
                date DATE NOT NULL,
                spotify_popularity INT,
                youtube_views BIGINT,
                youtube_likes BIGINT,
                youtube_comments BIGINT,
                itunes_rank INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP,
                CONSTRAINT uq_track_date UNIQUE(track_id, date),
                CONSTRAINT fk_track FOREIGN KEY(track_id)
                    REFERENCES dim.tracks(track_id)
                    ON DELETE CASCADE
            );
            """
            cur.execute(sql_query)
            conn.commit()

        disconnect_from_db(conn, cur)
    except Exception as e:
        logger.error(f"An error occurred during creating table: {schema}.{table} Error: {e}")
        raise e