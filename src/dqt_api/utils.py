"""

"""

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
