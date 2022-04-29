from functools import wraps

from .directory_files import delete_last_file_in_directory
from ..setup import SetUp




def delete_last_log_file(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        func(*args, **kwargs)
        delete_last_file_in_directory(SetUp.logs_dir)
    return wrapper

