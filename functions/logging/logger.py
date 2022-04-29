from ..setup import SetUp


def logger(message, level="INFO", category=4):
    """  """
    # setting logs to False means logs are turned off
    if not SetUp.logs:
        return
    
    from .structure import Logger
    msg = {
        1: f"\n\n{Logger.hashed}\n{message}\n{Logger.hashed}",
        2: f"\n{Logger.dashed} {message} {Logger.dashed}",
        3: f"\n{message} {Logger.dashed}",
        4: message
    }[category]

    logger = {
        "INFO": Logger.logger.info,
        "EXCEPTION": Logger.logger.exception,
        "WARNING": Logger.logger.warning,
        "ERROR": Logger.logger.error
    }[level]
    
    logger(msg)