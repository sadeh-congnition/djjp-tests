from time import sleep
from django.utils import timezone
from datetime import timedelta
from django_async_job_pipelines.jobs import job, run_job, lock_new_job_for_running
from django_async_job_pipelines.models import JobDBModel, LockedJob
from django_async_job_pipelines.db_layer import db as djjp_db
from django_async_job_pipelines.default_jobs import requeue_timed_out_jobs


@job(name="name234", timeout=10)
def func(a, b):
    """
    function implementation
    """
    sleep(1000)
    print("In func")
    print(a, b)


def test_no_timed_out_jobs_in_progress(db):
    j = func.run_later("a", b="b")
    j.should_run_by = timezone.now() - timedelta(seconds=1)
    j.save()

    # status is not IN_PROGRESS
    assert len(list(djjp_db.get_timed_out_jobs())) == 0


def test_no_timed_out_jobs(db):
    j = func.run_later("a", b="b")
    j.should_run_by = timezone.now() + timedelta(seconds=1)
    j.status = JobDBModel.Status.IN_PROGRESS
    j.save()

    # `should_run_by` is in future
    assert len(list(djjp_db.get_timed_out_jobs())) == 0


def test_timed_out_job_is_found(db):
    j = func.run_later("a", b="b")
    j.should_run_by = timezone.now() - timedelta(seconds=1)
    j.status = JobDBModel.Status.IN_PROGRESS
    j.save()

    assert len(list(djjp_db.get_timed_out_jobs())) == 1


def test_resetting_timed_out_job(db):
    j = func.run_later("a", b="b")
    j.should_run_by = timezone.now() - timedelta(seconds=1)
    j.status = JobDBModel.Status.IN_PROGRESS
    j.save()
    djjp_db.lock_job_by_id(j.id)

    j = JobDBModel.objects.get(id=j.id)
    assert j.manager
    assert j.status == JobDBModel.Status.IN_PROGRESS
    assert j.should_run_by
    assert LockedJob.objects.filter(job=j).count() == 1
    assert not j.messages

    requeue_timed_out_jobs()

    j = JobDBModel.objects.first()
    assert j.manager is None
    assert j.status == JobDBModel.Status.NEW
    assert j.should_run_by is None
    assert j.messages == [
        {
            "timestamp": timezone.now().strftime("%Y-%m-%d %H:%M:%S"),
            "type": "status change",
            "reason": "job timed out",
            "details": {"old_status": "IN_PROGRESS", "new_status": "NEW"},
        }
    ]
    assert LockedJob.objects.filter(job=j).count() == 0
