from loguru import logger

from dqt_api import db
from dqt_api import models


def save_data_model(graph_data):
    logger.info(f'Saving graph data.')
    dms = []
    for case, data in graph_data.items():
        dms.append(
            models.DataModel(
                case=case,
                age_bl=graph_data[case]['age_bl'],
                age_fu=graph_data[case]['age_fu'],
                sex=graph_data[case]['gender'],
                enrollment=graph_data[case]['enrollment'],
                followup_years=graph_data[case]['followup_years'])
        )
    db.session.bulk_save_objects(dms)
    db.session.commit()
    logger.info(f'Finished saving graph data.')
