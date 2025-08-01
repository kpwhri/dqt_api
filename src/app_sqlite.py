"""
Debugging for testdb with sqlite
"""
from pathlib import Path

from loguru import logger

from dqt_api.load_globals import initialize

logger.add('startup-logger-{time}.log', backtrace=True, diagnose=True)

from dqt_api.flask_logger import FlaskLoguru
from dqt_api import app, db, cors, whooshee
import os
import cherrypy
from paste.translogger import TransLogger


def mkdir_p(path):
    os.makedirs(path, exist_ok=True)
    return path


if not os.environ.get('FLASK_CONFIG_FILE', None):
    raise ValueError('Environment variable FLASK_CONFIG_FILE not set.')
app.config.from_pyfile(os.environ['FLASK_CONFIG_FILE'])

# check if db not specified
if not app.config.get('SQLALCHEMY_DATABASE_URI', None):
    path = max(Path(app.config['BASE_DIR']).glob('*.db'))
    app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{path}'

try:
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
    app.config['LOG_DIR'] = mkdir_p(os.path.join(app.config['BASE_DIR'], 'logs'))

    app.config['SQLALCHEMY_MIGRATE_REPO'] = mkdir_p(os.path.join(app.config['BASE_DIR'], 'migrations'))
    app.config['ALEMBIC'] = {
        'script_location': mkdir_p(os.path.join(app.config['BASE_DIR'], 'migrations')),
        'sqlalchemy.url': app.config['SQLALCHEMY_DATABASE_URI'],
    }
    if not app.config.get('WHOOSHEE_DIR', False):
        app.config['WHOOSH_BASE'] = mkdir_p(os.path.join(app.config['BASE_DIR'], 'whooshee.idx'))
        app.config['WHOOSHEE_DIR'] = mkdir_p(os.path.join(app.config['BASE_DIR'], 'whooshee.idx'))
    app.debug = True

    app.secret_key = app.config['SECRET_KEY']

    flask_logger = FlaskLoguru()
    flask_logger.init_app(app)
    app.logger.info('Initialized logger.')
except Exception as e:
    logger.exception(e)

db.init_app(app)
logger.info('Initialized database.')
# cors.init_app(app, resources={r'/api/*': {'origins': app.config['ORIGINS']}})
cors.init_app(app, resources={r'/api/*': {'origins': '*'}})

# noinspection PyUnresolvedReferences
import dqt_api.models
# noinspection PyUnresolvedReferences
import dqt_api.views

try:
    whooshee.init_app(app)
    whooshee.app = app
    app.logger.info('Initialized whooshee.')
    if not os.path.exists(os.path.join(app.config['WHOOSHEE_DIR'], 'category')):
        with app.app_context():
            whooshee.reindex()
        app.logger.info('Reindexed')
except Exception as e:
    logger.exception(e)
    app.logger.warning('Failed to initialize whooshee.')
    app.logger.exception(e)

with app.app_context():
    initialize(app, db)

app_logged = TransLogger(app, logger=app.logger, setup_console_handler=False)
cherrypy.tree.graft(app_logged, '/')
cherrypy.tree.mount(None, '/static', config={})
logger.debug(str(os.environ))
cherrypy.config.update(
    {
        'engine.autoreload.on': False,
        'log.screen': True,
        'server.socket_port': int(os.environ.get('XPORT', '8090')),
        'server.socket_host': '0.0.0.0',
    }
)

if hasattr(cherrypy.engine, 'signal_handler'):
    cherrypy.engine.signal_handler.subscribe()
cherrypy.engine.start()
cherrypy.engine.block()
