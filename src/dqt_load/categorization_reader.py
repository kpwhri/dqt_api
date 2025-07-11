import csv
from collections import namedtuple
from typing import Generator

from dqt_load.utils import clean_text_for_web

Row = namedtuple('Row', 'domain description name label priority values')


class CategorizationReader:

    def __init__(self, csv_file, encoding='utf8'):
        self.fh = open(csv_file, newline='', encoding=encoding)
        self.reader = csv.reader(self.fh)
        header = next(self.reader)
        self.data = {}
        for i, column in enumerate(header):
            column = column.lower()
            for label in {'name', 'label', 'domain', 'description', 'priority', 'values'} - self.data.keys():
                if label in column:  # variable name
                    self.data[label] = i

    def _get_data(self, row, label, default=None):
        try:
            return row[self.data[label]]
        except KeyError:
            if default is None:
                raise ValueError(f'Column {label} is required.')
            return default

    def __iter__(self) -> Generator[Row]:
        for row in self.reader:
            # enforce limits
            description = self._get_data(row, 'description')
            description = clean_text_for_web(description)
            if len(description) > 499:
                description = description[:496] + '...'
            yield Row(
                domain=self._get_data(row, 'domain'),
                description=description,
                name=clean_text_for_web(self._get_data(row, 'name').lower()),
                label=clean_text_for_web(self._get_data(row, 'label')),
                priority=int(self._get_data(row, 'priority', 0)),
                values=self._get_data(row, 'values', 0),
            )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.fh.close()
