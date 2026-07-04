import shutil

import pytest

from topx.utils.spark_session import get_spark_session


@pytest.fixture(scope="session")
def spark():
    session = get_spark_session(app_name="top-x-items-tests",
                                shuffle_partitions=2)
    yield session
    session.stop()


@pytest.fixture()
def tmp_output_dir(tmp_path):
    out_dir = tmp_path / "output"
    yield str(out_dir)
    shutil.rmtree(str(out_dir), ignore_errors=True)
