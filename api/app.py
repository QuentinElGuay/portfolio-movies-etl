import logging
import os
from pathlib import Path

from dotenv import load_dotenv
import duckdb
from flask import Flask, jsonify, g, request

from pagination import paginate_query


MOVIES_DB_PATH = Path(os.environ['MOVIES_DB_PATH'])

logging.basicConfig(level='INFO')

load_dotenv()

app = Flask(__name__)


def get_db():
    """Opens a unique database connection for the current web request thread."""

    if 'db' not in g:
        # Open connection in read-only mode if your API only queries data.
        # This prevents accidental locks. Remove 'read_only=True' if you must write data.
        g.db = duckdb.connect(MOVIES_DB_PATH, read_only=True)
    return g.db


@app.teardown_appcontext
def close_db(exception):
    """Automatically closes the database connection when the request finishes."""
    db = g.pop('db', None)
    if db is not None:
        db.close()


class IgnoreHealthFilter(logging.Filter):
    def filter(self, record):
        return '/health' not in record.getMessage()


werkzeug_logger = logging.getLogger('werkzeug')
werkzeug_logger.addFilter(IgnoreHealthFilter())


@app.route('/auth', methods=['POST'])
def authenticate():
    data = request.get_json() or {}
    username = data.get('username')
    password = data.get('password')

    # Simulation of authentication
    if username == os.getenv('USERNAME') and password == os.getenv('PASSWORD'):
        logging.info('AUTHENTICATION SUCCESS')
        return jsonify({'access_token': '123456', 'token_type': 'Bearer'}), 200

    # Return an error response for failed authentication
    logging.error('AUTHENTICATION FAIL')
    return jsonify({'error': 'Unauthorized', 'message': 'Credenciais inválidas'}), 401


@app.route('/art/v3/genres', methods=['GET'])
def list_genres():
    payload, next_url = paginate_query(
        conn=get_db(),
        data_query="""
        SELECT *
        FROM genres
        ORDER BY id
        """,
        count_query='SELECT COUNT(*) FROM genres',
        row_factory=lambda row: {
            'id': row[0],
            'name': row[1],
        },
    )

    response = jsonify(payload), 200
    if next_url:
        response.headers.add('Link', f'<{next_url}>; rel="next"')

    return response


@app.route('/art/v3/genres/<int:idGenre>/movies', methods=['GET'])
def get_genre_movies(idGenre: int):
    payload, next_url = paginate_query(
        conn=get_db(),
        data_query="""
        SELECT *
        FROM genres_movies
        WHERE genre_id = ?
        ORDER BY movie_id
        """,
        count_query='SELECT COUNT(*) FROM genres_movies',
        params=[idGenre],
        row_factory=lambda row: {  # TODO: check
            'movie_id': row[0],
            'genre_id': row[1],
        },
    )

    response = jsonify(payload), 200
    if next_url:
        response.headers.add('Link', f'<{next_url}>; rel="next"')

    return response


@app.route('/art/v3/movies', methods=['GET'])
def get_movies():
    payload, next_url = paginate_query(
        conn=get_db(),
        data_query="""
        SELECT *
        FROM movies
        ORDER BY id
        """,
        count_query='SELECT COUNT(*) FROM movies',
        row_factory=lambda row: {
            'id': row[0],
            'title': row[1],
            'year': row[2],
            'rating': row[3],
        },
    )

    response = jsonify(payload), 200
    if next_url:
        response.headers.add('Link', f'<{next_url}>; rel="next"')

    return response


@app.route('/art/v3/movies/<int:idMovie>', methods=['GET'])
def get_movie(idMovie: int):
    db = get_db()
    result = db.execute(
        """
        SELECT *
        FROM movies
        WHERE id = ?
        """,
        [idMovie],
    ).fetchdf()

    try:
        return jsonify(result.to_dict(orient='records')[0]), 200
    except IndexError:
        return jsonify({'error': 'Movie not found'}), 404


@app.route('/art/v3/movies/<int:idMovie>/ratings', methods=['GET'])
def get_movie_ratings(idMovie: int):
    payload, next_url = paginate_query(
        conn=get_db(),
        data_query="""
        SELECT *
        FROM ratings
        WHERE movie_id = ?
        ORDER BY id
        """,
        count_query='SELECT COUNT(*) FROM ratings',
        params=[idMovie],
        row_factory=lambda row: {
            'movie_id': row[0],
            'rating': row[1],
            'timestamp': row[3],
        },
    )

    response = jsonify(payload), 200
    if next_url:
        response.headers.add('Link', f'<{next_url}>; rel="next"')

    return response


@app.route('/art/v3/ratings', methods=['GET'])
def list_ratings():
    payload, next_url = paginate_query(
        conn=get_db(),
        data_query="""
        SELECT *
        FROM ratings
        ORDER BY timestamp ASC
        """,
        count_query='SELECT COUNT(*) FROM ratings',
        row_factory=lambda row: {
            'movie_id': row[0],
            'rating': row[1],
            'timestamp': row[2],
        },
    )

    response = jsonify(payload), 200
    if next_url:
        response.headers.add('Link', f'<{next_url}>; rel="next"')

    return response


@app.route('/health')
def health():
    return {'status': 'ok'}, 200


if __name__ == '__main__':
    app.run(
        host='0.0.0.0',
        port=5000,
        debug=False,
    )
