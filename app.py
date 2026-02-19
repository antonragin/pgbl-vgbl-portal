"""Flask app factory and entry point."""

import os
from flask import (Flask, g, render_template, request, redirect,
                   url_for, session, flash)
from database import get_db, init_db

SITE_PASSWORD = os.environ.get('SITE_PASSWORD', 'OryxRulezzz2026!')


def create_app():
    app = Flask(__name__)
    app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'prototype-key')
    app.config['DATABASE'] = 'data/portal.db'
    app.config['UPLOAD_FOLDER'] = 'uploads'
    app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max upload

    init_db(app)

    # Per-request DB connection + site-wide password gate
    @app.before_request
    def before_request():
        # Allow the gate page and static files without auth
        if request.endpoint in ('gate', 'static'):
            if request.endpoint != 'static':
                g.db = get_db(app)
            return
        if not session.get('site_authenticated'):
            return redirect(url_for('gate'))
        g.db = get_db(app)

    @app.teardown_request
    def teardown_request(exception):
        db = g.pop('db', None)
        if db is not None:
            db.close()

    # --- Site-wide password gate ---
    @app.route('/gate', methods=['GET', 'POST'])
    def gate():
        if session.get('site_authenticated'):
            return redirect(url_for('landing'))
        if request.method == 'POST':
            if request.form.get('password') == SITE_PASSWORD:
                session['site_authenticated'] = True
                return redirect(url_for('landing'))
            flash('Incorrect password.', 'error')
        return render_template('gate.html')

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
