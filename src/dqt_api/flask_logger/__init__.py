import os
import time
import zipfile
from datetime import datetime

from loguru import logger
from Crypto.Cipher import AES
from Crypto import Random

from .keys import log_dir, log_name, log_enqueue, log_format, log_key
from .keys import log_rotation, log_serialize


def archive_logs(config, file_list):
    dt = datetime.today().strftime('%Y%m%d_%H%M%S')
    fn = f'{config[log_name]}_{dt}.zip'
    fp = os.path.join(config[log_dir], fn)
    with zipfile.ZipFile(fp, 'w') as zipped:
        for tar in file_list:
            zipped.write(tar, os.path.basename(tar))
    encrypt_file(fp, config[log_key])
    for tar in file_list:
        os.remove(tar)


def pad(s):
    return s + b'\0' * (AES.block_size - len(s) % AES.block_size)


def encrypt(message, key, key_size=256):
    message = pad(message)
    iv = Random.new().read(AES.block_size)
    cipher = AES.new(key, AES.MODE_CBC, iv)
    return iv + cipher.encrypt(message)


def decrypt(ciphertext, key):
    iv = ciphertext[:AES.block_size]
    cipher = AES.new(key, AES.MODE_CBC, iv)
    plaintext = cipher.decrypt(ciphertext[AES.block_size:])
    return plaintext.rstrip(b'\0')


def encrypt_file(file_name, key):
    with open(file_name, 'rb') as fo:
        plaintext = fo.read()
    enc = encrypt(plaintext, key)
    with open(file_name + '.enc', 'wb') as fo:
        fo.write(enc)


def decrypt_file(file_name, key):
    with open(file_name, 'rb') as fo:
        ciphertext = fo.read()
    dec = decrypt(ciphertext, key)
    with open(file_name[:-4], 'wb') as fo:
        fo.write(dec)


class FlaskLoguru(object):

    def __init__(self, app=None, config=None):
        if not (config is None or isinstance(config, dict)):
            raise ValueError('`config` must be an instance of dict or None')

        self.config = config

        if app is not None:
            self.init_app(app, config)

    def init_app(self, app, config=None):
        if not (config is None or isinstance(config, dict)):
            raise ValueError('`config` must be an instance of dict or None')

        base_config = app.config.copy()
        if self.config:
            base_config.update(self.config)
        if config:
            base_config.update(config)

        config = base_config

        config.setdefault(log_dir, None)
        config.setdefault(log_name, 'dqt.log')
        config.setdefault(log_rotation, 60 * 60)
        config.setdefault(log_format, '')
        config.setdefault(log_enqueue, True)
        config.setdefault(log_serialize, True)
        if 'LOG_KEY' not in config:
            raise ValueError('LOG_KEY must be specified in Flask\'s config.py file! Use `os.urandom(32)` or see doco.')

        self._set_loguru(app, config)

    def _set_loguru(self, app, config):
        path = config[log_name]
        if config[log_dir] is not None:
            path = os.path.join(config[log_dir], config[log_name])

        def should_rotate(message, file):
            filepath = os.path.abspath(file.name)
            creation = os.path.getctime(filepath)
            now = message.record['time'].timestamp()
            return now - creation > config[log_rotation]

        def should_retention(logs):
            files = []
            for log in logs:
                if log.endswith(('.zip', '.enc')):
                    continue
                fp = os.path.abspath(log)
                if time.gmtime(time.time() - os.path.getctime(fp)).tm_mday == 7:
                    files.append(fp)
            if files:
                archive_logs(config, files)

        logger.add(path, format=config[log_format], rotation=should_rotate,
                   enqueue=config[log_enqueue], serialize=config[log_serialize],
                   retention=should_retention)

        if not hasattr(app, 'extensions'):
            app.extensions = {}

        app.extensions.setdefault('loguru', {})
        app.extensions['loguru'][self] = logger
