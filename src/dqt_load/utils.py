
TEXT_SUBS = {
    '”': '"',
    '“': '"',
    "’": "'",
    "‘": "'",
    '||||': '\n',
    '||': '\n',
    '\u2265': '>=',
    '\u2264': '<=',
}

def clean_text_for_web(text):
    if isinstance(text, str):
        for src, repl in TEXT_SUBS.items():
            text = text.replace(src, repl)
    return text



def clean_number(value):
    """Remove spaces and dollar signs to convert to numeric"""
    if isinstance(value, (int, float)):
        return value
    return value.replace(',', '').replace('$', '').strip()


def line_not_empty(lst):
    """Check if list is not empty and first value is not empty"""
    return bool(lst and lst[0])


def transform_decimal(num):
    res = str(num)
    if '.' in res:
        return res[:res.index('.') + 2]
    return res
