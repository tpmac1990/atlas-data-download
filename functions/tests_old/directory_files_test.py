import pytest

from .directory_files import delete_files_in_directory, archive_and_clear_directories


@pytest.fixture
def tmp_dir(tmpdir):
    f1 = tmpdir / "mydir"
    f1.mkdir()
    return f1


def test_delete_files_in_directory_incorrect_path():
    with pytest.raises(FileNotFoundError):
        delete_files_in_directory('blahblah')
    
def test_delete_files_in_directory_correct_path(tmp_dir):
    delete_files_in_directory(tmp_dir)


def test_archive_and_clear_directories():
    archive_and_clear_directories(
            archive_lst=[], 
            delete_lst=[], 
            p_src_dir='', 
            p_dest_dir=''
        )

