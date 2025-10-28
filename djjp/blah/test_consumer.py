import pytest
from django import db
import subprocess as sp
from threading import Thread
from multiprocessing import Process
import pytest


from django_async_job_pipelines.models import JobDBModel
from django_async_job_pipelines.consumers import run_default


@pytest.fixture
def consumer(db):
    # cons = Process(target=call_command, args=["run_consumer"])
    cons = Thread(target=run_default, args=[1])
    return cons


def est_consume_one_job(job1, consumer):
    j_created = job1.run_later("a", "b", "c", d="d")
    # consumer.start()
    # print("Consumer is running")
    # sleep(2)
    # consumer.terminate()
    #
    # sp.run(["uv", "run", "manage.py", "make_tasks"])
    # with pytest.raises(sp.TimeoutExpired):
    #     sp.run(["uv", "run", "manage.py", "run_consumer"], timeout=2)
    #
    db.connections.close_all()
    consumer.start()
    consumer.join()

    j = JobDBModel.objects.get(id=j_created.id)
    assert j.status == JobDBModel.Status.DONE
