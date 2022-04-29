import os
import pytest
from pathlib import Path

from functions.common.directory_files import delete_last_file_in_directory


@pytest.fixture
def tmp_file(tmpdir):
    d = tmpdir.mkdir("sub")
    p = d.join("test.log")
    p.write("content")
    return d, p


class DeleteLastLogFileTests:
    """
    pytest -s functions/tests/directory_files_tests.py
    """
    
    def delete_last_log_file_test(self, tmp_file):
        d, p = tmp_file
        result = delete_last_file_in_directory(d)
        assert result
        assert os.path.exists(p) == 0

    def delete_none_if_no_file_exists_test(self, tmpdir):
        d = tmpdir.mkdir("sub")
        result = delete_last_file_in_directory(d)
        assert not result

        

        
        
        
        