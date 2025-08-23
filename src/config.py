import os

def load_config():
    env = os.getenv("PROJECT_ENV", "dev").lower()
    if env == "dev":
        from conf.config_dev import Config
    elif env == "qa":
        from conf.config_qa import Config
    elif env == "prod":
        from conf.config_prod import Config
    else:
        raise Exception(f"Invalid environment: {env}")

    return Config
