import os
import pickle

from dqt_api import scheduler, models
from dqt_api.views import get_all_categories, api_filter_chart_helper, remove_values


def initialize(app, db):
    """Initialize starting values."""
    scheduler.scheduler.add_job(scheduler.remove_old_logs, 'cron', day_of_week=6, id='remove_old_logs')
    dump_file = os.path.join(app.config['BASE_DIR'], 'dump.pkl')
    app.logger.info('Attempting to load data from previous cache...')
    try:
        with open(dump_file, 'rb') as fh:
            population_size, precomputed_column, precomputed_filter, null_filter = pickle.load(fh)
            app.config['POPULATION_SIZE'] = population_size
            app.config['PRECOMPUTED_COLUMN'] = precomputed_column
            app.config['PRECOMPUTED_FILTER'] = precomputed_filter
            app.config['NULL_FILTER'] = null_filter

        app.logger.info(f'Loaded from file: {dump_file}')
        return
    except Exception as e:
        app.logger.info(f'Failed to load file cache, rebuilding: {e}')
    app.logger.info('Building cache: this may take a few minutes.')
    app.logger.debug('Initializing...loading population size...')
    app.config['POPULATION_SIZE'] = db.session.query(models.DataModel).count()
    app.logger.debug('Initializing...precomputing categories...')
    app.config['PRECOMPUTED_COLUMN'] = get_all_categories()
    app.logger.debug('Initializing...building indices...')
    app.config['PRECOMPUTED_FILTER'] = api_filter_chart_helper(jitter=False)
    app.logger.debug('Initializing...building null index...')
    app.config['NULL_FILTER'] = remove_values(app.config['PRECOMPUTED_FILTER'])
    app.logger.debug('Finished initializing...')

    try:
        with open(dump_file, 'wb') as fh:
            pickle.dump((
                app.config['POPULATION_SIZE'],
                app.config['PRECOMPUTED_COLUMN'],
                app.config['PRECOMPUTED_FILTER'],
                app.config['NULL_FILTER']),
                fh
            )
    except Exception as e:
        app.logger.exception('Failed to write to dump file: {}'.format(e))
