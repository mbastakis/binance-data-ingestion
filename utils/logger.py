import logging

def get_logger(name):
    logger = logging.getLogger(name)
    if not logger.handlers:
        # Avoid adding multiple handlers if the logger already has them
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s %(levelname)s [%(name)s]: %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger
