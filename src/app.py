import logging
logging.basicConfig(  # troubleshoot initialization issues
    filename=r'C:\wksp\log.txt',
    filemode='a',
    level=logging.DEBUG
)
from dqt_api import app, cors, whooshee
import dqt_api.models
import dqt_api.views
import os
import cherrypy
from dqt_api import mylogging
from paste.translogger import TransLogger


def mkdir_p(path):
    os.makedirs(path, exist_ok=True)
    return path


if not os.environ.get('FLASK_CONFIG_FILE', None):
    raise ValueError('Environment variable FLASK_CONFIG_FILE not set.')
app.config.from_pyfile(os.environ['FLASK_CONFIG_FILE'])
try:
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
    app.config['LOG_DIR'] = mkdir_p(os.path.join(app.config['BASE_DIR'], 'logs'))
    app.config['SQLALCHEMY_MIGRATE_REPO'] = mkdir_p(os.path.join(app.config['BASE_DIR'], 'migrations'))
    # app.config['WHOOSH_BASE'] = mkdir_p(os.path.join(app.config['BASE_DIR'], 'whooshee.idx'))
    # app.config['WHOOSHEE_DIR'] = mkdir_p(os.path.join(app.config['BASE_DIR'], 'whooshee.idx'))
    app.config['ALEMBIC'] = {
        'script_location': mkdir_p(os.path.join(app.config['BASE_DIR'], 'migrations')),
        'sqlalchemy.url': app.config['SQLALCHEMY_DATABASE_URI'],
    }
    app.config['WHOOSHEE_DIR'] = r'C:\wksp\dqt_api\src\whooshee'
    app.debug = True

    app.secret_key = app.config['SECRET_KEY']

    handler = mylogging.EncryptedTimedRotatingFileHandler(
        os.path.join(app.config['LOG_DIR'], 'log_file'),
        app.config.get('LOG_KEY', None),
        "midnight",
        1
    )
    app.logger.addHandler(handler)
except Exception as e:
    logging.error(e)

cors.init_app(app, resources={r'/api/*': {'origins': app.config['ORIGINS']}})

try:
    whooshee.init_app(app)
    whooshee.app = app
except Exception as e:
    print('Failed to initialize whooshee.')
    print(e)
    logger.error(e)
    app.logger.warning('Failed to initialize whooshee.')
    app.logger.exception(e)

app_logged = TransLogger(app, logger=app.logger, setup_console_handler=False)
cherrypy.tree.graft(app_logged, '/')
cherrypy.tree.mount(None, '/static', config={})
logging.debug(os.environ)
cherrypy.config.update(
    {
        'engine.autoreload.on': False,
        'log.screen': True,
        'server.socket_port': int(os.environ['XPORT']),
        'server.socket_host': '0.0.0.0',
    }
)

if hasattr(cherrypy.engine, 'signal_handler'):
    cherrypy.engine.signal_handler.subscribe()
cherrypy.engine.start()
cherrypy.engine.block()
