import os
import logging

from dotenv import load_dotenv
import duckdb
from flask import Flask, jsonify, g, request

DATABASE_PATH = 'data/movies.db'

logging.basicConfig(level='INFO')

load_dotenv()

app = Flask(__name__)


def get_db():
    """Opens a unique database connection for the current web request thread."""

    if 'db' not in g:
        # Open connection in read-only mode if your API only queries data.
        # This prevents accidental locks. Remove 'read_only=True' if you must write data.
        g.db = duckdb.connect(DATABASE_PATH, read_only=True)
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

    # Validação simples de credenciais simuladas
    if username == os.getenv('USERNAME') and password == os.getenv('PASSWORD'):
        logging.info('AUTHENTICATION SUCCESS')
        return jsonify({'access_token': '123456', 'token_type': 'Bearer'}), 200

    # Retorna erro caso as credenciais estejam erradas
    logging.error('AUTHENTICATION FAIL')
    return jsonify({'error': 'Unauthorized', 'message': 'Credenciais inválidas'}), 401


@app.route('/art/v3/genres', methods=['GET'])
def list_genres():
    db = get_db()

    query = 'SELECT * FROM genres'
    result = db.execute(query).fetchall()

    genres_list = [{'id': row[0], 'name': row[1]} for row in result]
    return jsonify(genres_list), 200


# Simulate a GET request endpoint
@app.route('/art/v3/genres/<int:idGenre>/movies', methods=['GET'])
def get_genre_movies(idGenre: int):
    db = get_db()

    query = 'SELECT * FROM genres_movies WHERE genre_id = ?'
    movies_list = db.execute(query, [idGenre]).fetchall()

    if movies_list:
        return jsonify(movies_list), 200
    return jsonify({'error': 'Genre not found'}), 404


@app.route('/art/v3/movies/<int:idMovie>', methods=['GET'])
def get_movie(idMovie: int):
    db = get_db()

    query = 'SELECT * FROM movies WHERE movie_id = ?'
    movie = db.execute(query, [idMovie]).fetch()

    if movie:
        return jsonify(movie), 200
    return jsonify({'error': 'Movie not found'}), 404


@app.route('/art/v3/movies/<int:idMovie>/ratings', methods=['GET'])
def get_movie_ratings(idMovie: int):
    db = get_db()

    query = 'SELECT * FROM ratings WHERE movie_id = ?'
    ratings_list = db.execute(query, [idMovie]).fetchall()

    return jsonify(ratings_list), 200


@app.route('/health')
def health():
    return {'status': 'ok'}, 200


if __name__ == '__main__':
    app.run(
        host='0.0.0.0',
        port=5000,
        debug=False,
    )
