import yaml
import os

class ConfigLoader:
    @staticmethod
    def load_config(config_file='config.yml'):
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)
        # Override config with environment variables if they exist
        db_config = config.get('database', {})
        db_config['host'] = os.environ.get('DATABASE_HOST', db_config.get('host', 'localhost'))
        db_config['port'] = int(os.environ.get('DATABASE_PORT', db_config.get('port', 5432)))
        db_config['user'] = os.environ.get('DATABASE_USER', db_config.get('user', 'postgres'))
        db_config['password'] = os.environ.get('DATABASE_PASSWORD', db_config.get('password', 'postgres'))
        db_config['dbname'] = os.environ.get('DATABASE_NAME', db_config.get('dbname', 'postgres'))
        config['database'] = db_config
        return config
