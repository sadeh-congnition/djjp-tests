import pytest
from django_async_job_pipelines.jobs import job


@job(name="name5", timeout=10)
def func(a, b):
    """
    function implementation
    """
    print("In func")
    print(a, b)


@pytest.fixture
def job1():
    return func


#
# @pytest.fixture(scope="session")
# def django_db_setup():
#     pass
#
#
# @pytest.fixture(scope="session")
# def django_db_modify_db_settings():
#     pass
