"""Flask app factory and entry point."""

import os
from flask import Flask, g, render_template, request, abort
from database import get_db, init_db


def create_app():
    app = Flask(__name__)
    app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'prototype-key')
    app.config['DATABASE'] = 'data/portal.db'
    app.config['UPLOAD_FOLDER'] = 'uploads'
    app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max upload

    init_db(app)

    # Per-request DB connection
    @app.before_request
    def before_request():
        g.db = get_db(app)

    @app.teardown_request
    def teardown_request(exception):
        db = g.pop('db', None)
        if db is not None:
            db.close()

    # Register blueprints
    from blueprints.admin import admin_bp
    from blueprints.portal import portal_bp
    app.register_blueprint(admin_bp, url_prefix='/admin')
    app.register_blueprint(portal_bp, url_prefix='/portal')

    # Landing page
    @app.route('/')
    def landing():
        return render_template('landing.html')

    return app


# Module-level app instance for gunicorn: gunicorn app:app
app = create_app()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
