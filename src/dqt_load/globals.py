from collections import defaultdict

COLUMN_TO_DOMAIN = {}
COLUMN_TO_DESCRIPTION = {}
COLUMN_TO_LABEL = {}  # column name to "label" (the visible piece)

DOMAINS = {}  # name -> models.Category

VALUES = defaultdict(dict)  # name -> item -> models.Value
VALUES_BY_ITEM = defaultdict(dict)  # item -> id -> models.Value; for pre-defined ids

ITEMS = {}  # name -> models.Item