from data_utils import connect_to_db, disconnect_from_db
import logging

logger = logging.getLogger(__name__)

def create_artist(artist_name):
    conn, cur = None, None
    try:
        conn, cur = connect_to_db()

        sql_query = """
        INSERT INTO dim.artists(artist_name)
        VALUES(%s);
        """
        cur.execute(sql_query, (artist_name,))
        conn.commit()

        disconnect_from_db(conn, cur)
        logger.info(f"Artist: {artist_name} created.")
    except Exception as e:
        logger.error(f"An error occurred during inserting artist: {artist_name} Error: {e}")
        raise e
    
