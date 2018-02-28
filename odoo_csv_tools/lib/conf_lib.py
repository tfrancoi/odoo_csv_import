import odoolib
import sys
if sys.version_info >= (3, 0, 0):
    import configparser as ConfigParser
else:
    import ConfigParser
import logging
import sys


def get_server_connection(config_file):
    config = ConfigParser.RawConfigParser({'protocol' : 'xmlrpc', 'port' : 8069})
    config.read(config_file)

    hostname = config.get('Connection', 'hostname')
    database = config.get('Connection', 'database')
    login = config.get('Connection', 'login')
    password = config.get('Connection', 'password')
    protocol = config.get('Connection', 'protocol')
    port = int(config.get('Connection', 'port'))
    uid = int(config.get('Connection', 'uid'))
    return odoolib.get_connection(hostname=hostname, database=database, login=login, password=password, protocol=protocol, port=port, user_id=uid)

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
