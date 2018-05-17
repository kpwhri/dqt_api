import argparse

parser = argparse.ArgumentParser(fromfile_prefix_chars='@!')
# data dictionary loading options
parser.add_argument('--dd-input-file', nargs='+',
                    help='Path to excel document(s) containing data dictionary.'
                         ' Values are expected to be contained in the first table'
                         ' in the file.')
parser.add_argument('--dd-file-name', default='data-dictionary.xlsx',
                    help='Displayed filename for downloads of the data dictionary')
parser.add_argument('--dd-label-column', required=True,
                    help='Name of label column in document; the label is the common name')
parser.add_argument('--dd-category-column',
                    help='Name of category column in document; if you want to use '
                         'excel tabs with categories, do not include this option')
parser.add_argument('--dd-name-column',
                    help='Name of variable name column in document; variable name as opposed to common name')
parser.add_argument('--dd-description-column',
                    help='Name of description column in document; should discuss details of the variable')
parser.add_argument('--dd-value-column',
                    help='Name of values column in document; what the possible values are for the label')
