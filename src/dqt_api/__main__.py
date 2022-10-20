"""
Entry point for starting the application along with `app.py`.

They're meant to be more or less identical, but there may be some differences between the two.
"""
import logging

from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from tornado.wsgi import WSGIContainer

from dqt_api import app, db, cors, whooshee

import os
import cherrypy
import argparse
from dqt_api import mylogging
from paste.translogger import TransLogger

from dqt_api.flask_logger import FlaskLoguru


def mkdir_p(path):
    os.makedirs(path, exist_ok=True)
    return path


def prepare_config(debug=False, whooshee_dir=False):
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
    app.config['LOG_DIR'] = mkdir_p(os.path.join(app.config['BASE_DIR'], 'logs'))
    app.config['SQLALCHEMY_MIGRATE_REPO'] = mkdir_p(os.path.join(app.config['BASE_DIR'], 'migrations'))
    if whooshee_dir:
        app.config['WHOOSH_BASE'] = mkdir_p(os.path.join(app.config['BASE_DIR'], 'whooshee.idx'))
        app.config['WHOOSHEE_DIR'] = mkdir_p(os.path.join(app.config['BASE_DIR'], 'whooshee.idx'))
    app.config['ALEMBIC'] = {
        'script_location': mkdir_p(os.path.join(app.config['BASE_DIR'], 'migrations')),
        'sqlalchemy.url': app.config['SQLALCHEMY_DATABASE_URI'],
    }
    app.debug = debug

    app.secret_key = app.config['SECRET_KEY']

    flask_logger = FlaskLoguru()
    flask_logger.init_app(app)

    db.init_app(app)

    cors.init_app(app, resources={r'/api/*': {'origins': app.config['ORIGINS']}})

    # noinspection PyUnresolvedReferences
    import dqt_api.models
    # noinspection PyUnresolvedReferences
    import dqt_api.views

    try:
        whooshee.init_app(app)
        whooshee.app = app  # needs to be done manually
        app.logger.info('Initialized whooshee.')
        if not os.path.exists(os.path.join(app.config['WHOOSHEE_DIR'], 'category')):
            with app.app_context():
                whooshee.reindex()
            app.logger.info('Reindexed')
    except Exception as e:
        app.logger.warning('Failed to initialize whooshee.')
        app.logger.exception(e)


def run_cherrypy_server(port=8090):
    app_logged = TransLogger(app, logger=app.logger, setup_console_handler=False)
    cherrypy.tree.graft(app_logged, '/')
    cherrypy.tree.mount(None, '/static', config={})
    cherrypy.config.update(
        {
            'engine.autoreload.on': False,
            'log.screen': True,
            'server.socket_port': port,
            'server.socket_host': '0.0.0.0',
        }
    )
    if hasattr(cherrypy.engine, 'signal_handler'):
        cherrypy.engine.signal_handler.subscribe()
    cherrypy.engine.start()
    cherrypy.engine.block()


def run_tornado_server(port=8090):
    """
    TODO: enable logging
    :param port:
    :return:
    """
    server = HTTPServer(WSGIContainer(app))
    server.listen(port)
    IOLoop.instance().start()


def main():
    server_choices = ('cherrypy', 'tornado')
    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    parser.add_argument('--port', default=8090, type=int,
                        help='Specify port to run file on.')
    parser.add_argument('--config', required=True,
                        help='File containing configuration information. '
                             'BASE_DIR, SECRET_KEY, AGE_STEP, AGE_MAX, AGE_MIN, MASK.')
    parser.add_argument('--debug', default=False, action='store_true',
                        help='Run in debug mode.')
    parser.add_argument('--whooshee-dir', default=False, action='store_true',
                        help='Use whooshee directory in BASE_DIR.')
    parser.add_argument('--server', choices=server_choices, default=server_choices[0],
                        help='Select server to run.')
    args = parser.parse_args()

    app.config.from_pyfile(args.config)
    prepare_config(args.debug, args.whooshee_dir)
    cors.init_app(app, resources={r'/api/*': {'origins': app.config['ORIGINS']}})

    if args.server == 'cherrypy':
        run_cherrypy_server(port=args.port)
    elif args.server == 'tornado':
        run_tornado_server(port=args.port)


if __name__ == '__main__':
    main()
