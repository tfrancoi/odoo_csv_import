import openerplib
import ConfigParser
import logging
import sys

from . import internal  # noqa
from . import workflow  # noqa
from . import checker  # noqa
from . import mapper  # noqa
from . import transform  # noqa

def config_file_parse(config_file):
    config = ConfigParser.RawConfigParser({'protocol': 'xmlrpc', 'port': 8069})
    config.read(config_file)
    return {
        'hostname': config.get('Connection', 'hostname'),
        'database': config.get('Connection', 'database'),
        'login': config.get('Connection', 'login'),
        'password': config.get('Connection', 'password'),
        'protocol': config.get('Connection', 'protocol'),
        'port': int(config.get('Connection', 'port')),
        'user_id': int(config.get('Connection', 'uid')),
    }


def get_server_connection(config={}):
    config.setdefault('hostname', 'localhost')
    config.setdefault('login', 'admin')
    config.setdefault('password', 'admin')
    config.setdefault('user_id', 1)
    config.setdefault('protocol', 'xmlrpc')
    config.setdefault('port', 8069)
    return openerplib.get_connection(**config)


def init_logger():
    logger_err = logging.getLogger("error")
    logger_err.setLevel(logging.INFO)
    err = logging.StreamHandler(sys.stderr)
    logger_err.addHandler(err)
    logger = logging.getLogger("info")
    logger.setLevel(logging.INFO)
    out = logging.StreamHandler(sys.stdout)
    logger.addHandler(out)


def log_info(msg):
    logging.getLogger("info").info(msg)


def log_error(msg):
    logging.getLogger("error").info(msg)


def log(msg):
    log_info(msg)
    log_error(msg)


init_logger()
