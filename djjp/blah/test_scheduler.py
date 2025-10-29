import pytest
from time import sleep
from datetime import timedelta
from django.utils import timezone
from django_async_job_pipelines.jobs import job
from django_async_job_pipelines.models import ScheduledJob
from django_async_job_pipelines.scheduler import (
    Scheduler,
    Every,
    registry,
    get_scheduled_jobs_to_run,
)


@job(name="name22", timeout=10)
def func(a, b):
    """
    function implementation
    """
    print("In func")
    print(a, b)


def funcs2(a, b):
    """
    function implementation
    """


@pytest.fixture
def scheduler_registry():
    yield registry
    registry.clear()


def test_creating_duplicate_schedule_name(db, scheduler_registry):
    with pytest.raises(ValueError):
        with Scheduler() as sched:
            sched.add(name="sched1", job=func, interval=Every().seconds(1))
            sched.add(name="sched1", job=func, interval=Every().seconds(1))


def test_creating_schedule_for_non_job_function(db, scheduler_registry):
    with pytest.raises(TypeError):
        with Scheduler() as sched:
            sched.add(name="sched1", job=funcs2, interval=Every().seconds(1))


def test_add_schedule_for_one_job(db, scheduler_registry):
    with Scheduler() as sched:
        sched.add(name="sched1", job=func, interval=Every().seconds(1))

    assert ScheduledJob.objects.count() == 1

    sch_job = ScheduledJob.objects.first()
    assert sch_job.name == "sched1"
    assert sch_job.job_name == "name22"
    assert sch_job.interval == Every().seconds(1).to_dict()


def test_add_schedule_for_two_jobs(db, scheduler_registry):
    with Scheduler() as sched:
        sched.add(name="sched1", job=func, interval=Every().seconds(1))
        sched.add(name="sched2", job=func, interval=Every().seconds(2))

    assert ScheduledJob.objects.count() == 2

    sch_job = ScheduledJob.objects.first()
    assert sch_job.name == "sched1"
    assert sch_job.job_name == "name22"
    assert sch_job.interval == Every().seconds(1).to_dict()

    sch_job = ScheduledJob.objects.last()
    assert sch_job.name == "sched2"
    assert sch_job.job_name == "name22"
    assert sch_job.interval == Every().seconds(2).to_dict()


def test_update_schedule_for_one_job(db, scheduler_registry):
    with Scheduler() as sched:
        sched.add(name="sched1", job=func, interval=Every().seconds(1))

    scheduler_registry.clear()  # removes schedule name from registry

    with Scheduler() as sched:
        sched.add(name="sched1", job=func, interval=Every().seconds(2))

    assert ScheduledJob.objects.count() == 1

    sch_job = ScheduledJob.objects.first()
    assert sch_job.name == "sched1"
    assert sch_job.job_name == "name22"
    assert sch_job.interval == Every().seconds(2).to_dict()


def test_schedule_not_in_registry_but_in_db_gets_removed(db, scheduler_registry):
    """This tests the scheduler context manager's `__exit__`"""
    with Scheduler() as sched:
        sched.add(name="sched1", job=func, interval=Every().seconds(1))

    scheduler_registry.clear()  # removes schedule name from registry

    with Scheduler() as sched:
        sched.add(name="sched2", job=func, interval=Every().seconds(2))

    assert ScheduledJob.objects.count() == 1

    sch_job = ScheduledJob.objects.first()
    assert sch_job.name == "sched2"
    assert sch_job.job_name == "name22"
    assert sch_job.interval == Every().seconds(2).to_dict()


def test_scheduled_job_immediate_first_run(db, scheduler_registry):
    with Scheduler() as sched:
        sched.add(
            name="sched1",
            job=func,
            interval=Every().seconds(10),
            trigger_first_run_now=True,
        )

    sch_job = ScheduledJob.objects.first()
    assert sch_job.run_ts < timezone.now() - timedelta(seconds=10)


def test_no_jobs_due_to_run(db, scheduler_registry):
    with Scheduler() as sched:
        sched.add(name="sched1", job=func, interval=Every().seconds(3))
    sleep(2)
    jobs = [j for j in get_scheduled_jobs_to_run()]

    assert len(jobs) == 0


def test_one_job_due_to_run(db, scheduler_registry):
    with Scheduler() as sched:
        sched.add(name="sched1", job=func, interval=Every().seconds(1))
    sleep(2)
    jobs = [j for j in get_scheduled_jobs_to_run()]

    assert len(jobs) == 1
