import logging


def warn(message, category=None, stacklevel=1):
    m = message
    if isinstance(category, type):
        m = '{}({})'.format(category.__name__, m)
    logger = logging.getLogger('luigi-interface')
    if not logger.handlers:
        logger.addHandler(logging.StreamHandler())
    logger.warning(m)
