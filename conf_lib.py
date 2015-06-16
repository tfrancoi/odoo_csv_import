import openerplib
import ConfigParser
        

def get_server_connection(config_file):
    config = ConfigParser.RawConfigParser({'protocol' : 'xmlrpc', 'port' : 8069})
    config.read(config_file)

    hostname = config.get('Connection', 'hostname')
    database = config.get('Connection', 'database')
    login = config.get('Connection', 'login')
    password = config.get('Connection', 'password')
    protocol = config.get('Connection', 'protocol')
    port = int(config.get('Connection', 'port'))
    #uid = int(config.get('Connection', 'uid'))
    return openerplib.get_connection(hostname=hostname, database=database, login=login, password=password, protocol=protocol, port=port)
    

def get_file(config_file):
    config = ConfigParser.RawConfigParser()
    config.read(config_file)
    return config.get('Import', 'csv_file')
    
    
def get_model(config_file):
    config = ConfigParser.RawConfigParser()
    config.read(config_file)
    return config.get('Import', 'model')

def get_batch_size(config_file):
    config = ConfigParser.RawConfigParser()
    config.read(config_file)
    return config.getint('Import', 'batch_size')
    
def get_faile_file(config_file):
    config = ConfigParser.RawConfigParser()
    config.read(config_file)
    return config.get('Import', 'fail_file')
    
def get_max_connection(config_file):
    config = ConfigParser.RawConfigParser()
    config.read(config_file)
    return config.get('Import', 'max_connection')
