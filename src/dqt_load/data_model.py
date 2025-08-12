from loguru import logger

from dqt_api import db
from dqt_api import models


def apply_mapping(value, mapping):
    if mapping is not None:
        return mapping.get(value, value)
    return value

def read_mapping(mapping):
    if mapping:
        result = {}
        for m in mapping.replace('_', ' ').split('&'):
            k, v = m.split('==')
            result[k] = v
            result[int(k)] = v
        return result
    return mapping

def save_data_model(graph_data, *, enrollment_mapping=None, gender_mapping=None):
    logger.info(f'Saving graph data.')
    enrollment_mapping = read_mapping(enrollment_mapping)
    gender_mapping = read_mapping(gender_mapping)
    dms = []
    for case, data in graph_data.items():
        dms.append(
            models.DataModel(
                case=case,
                age_bl=graph_data[case]['age_bl'],
                age_fu=graph_data[case]['age_fu'],
                sex=apply_mapping(graph_data[case]['gender'], gender_mapping),
                enrollment=apply_mapping(graph_data[case]['enrollment'], enrollment_mapping),
                followup_years=graph_data[case]['followup_years'])
        )
    db.session.bulk_save_objects(dms)
    db.session.commit()
    logger.info(f'Finished saving graph data.')
