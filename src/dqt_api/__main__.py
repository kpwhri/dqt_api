from dqt_api import app, db
import dqt_api.models
import dqt_api.views
import os
import cherrypy
import argparse
from dqt_api import mylogging
from paste.translogger import TransLogger


def mkdir_p(path):
    os.makedirs(path, exist_ok=True)
    return path


def prepare_config(debug=False):
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
    app.config['LOG_DIR'] = mkdir_p(os.path.join(app.config['BASE_DIR'], 'logs'))
    app.config['SQLALCHEMY_MIGRATE_REPO'] = mkdir_p(os.path.join(app.config['BASE_DIR'], 'migrations'))
    app.config['WHOOSH_BASE'] = mkdir_p(os.path.join(app.config['BASE_DIR'], 'whoosh.idx'))
    app.config['WHOOSHEE_DIR'] = mkdir_p(os.path.join(app.config['BASE_DIR'], 'whooshee.idx'))
    app.config['ALEMBIC'] = {
        'script_location': mkdir_p(os.path.join(app.config['BASE_DIR'], 'migrations')),
        'sqlalchemy.url': app.config['SQLALCHEMY_DATABASE_URI'],
    }
    app.debug = debug

    app.secret_key = app.config['SECRET_KEY']

    handler = mylogging.StaticTimedRotatingFileHandler(
        os.path.join(app.config['LOG_DIR'], 'log_file'), "midnight", 1)
    handler.suffix = '%Y-%m-%d'
    app.logger.addHandler(handler)


def run_server(port=8090):
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


def main():
    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    parser.add_argument('--port', default=8090, type=int,
                        help='Specify port to run file on.')
    parser.add_argument('--config', required=True,
                        help='File containing configuration information. '
                             'BASE_DIR, SECRET_KEY, AGE_STEP, AGE_MAX, AGE_MIN, MASK.')
    parser.add_argument('--debug', default=False, action='store_true',
                        help='Run in debug mode.')
    args = parser.parse_args()

    app.config.from_pyfile(args.config)
    prepare_config(args.debug)
    run_server(port=args.port)


if __name__ == '__main__':
    main()
