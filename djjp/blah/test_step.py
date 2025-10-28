import pytest

from django_async_job_pipelines.jobs import job, run_job, lock_new_job_for_running
from django_async_job_pipelines.models import JobDBModel, LockedJob
from django_async_job_pipelines.steps import Step


@job(name="name1", timeout=10)
def func(a, b):
    """
    function implementation
    """
    print("In func")
    print(a, b)


@job(name="name2", timeout=10)
def funcs():
    """
    function implementation
    """


def non_job_func():
    pass


def test_add_non_job_to_step():
    step = Step()
    with pytest.raises(TypeError):
        step.add_job(non_job_func, "a", b="b")


def test_step_with_one_job(db):
    step = Step()
    j_created = step.add_job(func, "a", b="b")

    j = JobDBModel.objects.get(id=j_created.id)

    assert JobDBModel.objects.filter(status=JobDBModel.Status.NEW).count() == 1
    assert JobDBModel.objects.first().next_step is None
    assert j.step == j_created.step


def test_run_step_with_one_job(db):
    step = Step()
    j = step.add_job(func, "a", b="b")

    run_job(j)

    assert JobDBModel.objects.filter(status=JobDBModel.Status.NEW).count() == 0
    assert JobDBModel.objects.filter(status=JobDBModel.Status.DONE).count() == 1
    assert JobDBModel.objects.first().next_step is None


def test_step_with_multiple_jobs(db):
    step = Step()
    step.add_job(func, "a", b="b")
    step.add_job(funcs)

    assert JobDBModel.objects.filter(status=JobDBModel.Status.NEW).count() == 2
    assert JobDBModel.objects.first().next_step is None
    assert JobDBModel.objects.last().next_step is None


def test_run_step_with_multiple_jobs(db):
    step = Step()
    j1 = step.add_job(func, "a", b="b")
    j2 = step.add_job(funcs)

    run_job(j1)

    assert JobDBModel.objects.filter(status=JobDBModel.Status.NEW).count() == 1
    assert JobDBModel.objects.filter(status=JobDBModel.Status.DONE).count() == 1
    assert JobDBModel.objects.first().next_step is None
    assert JobDBModel.objects.last().next_step is None

    run_job(j2)

    assert JobDBModel.objects.filter(status=JobDBModel.Status.NEW).count() == 0
    assert JobDBModel.objects.filter(status=JobDBModel.Status.DONE).count() == 2


def test_two_steps(db):
    step1 = Step()
    j_created = step1.add_job(func, "a", b="b")

    step2 = step1.create_next_step()
    j2_created = step2.add_job(funcs)

    assert JobDBModel.objects.filter(status=JobDBModel.Status.NEW).count() == 1
    assert JobDBModel.objects.filter(status=JobDBModel.Status.BLOCKED).count() == 1

    j1 = JobDBModel.objects.get(id=j_created.id)
    j2 = JobDBModel.objects.get(id=j2_created.id)

    assert j1.step == step1.id
    assert j1.next_step == step2.id
    assert j2.step == step2.id
    assert j2.next_step is None


def test_run_two_steps(db):
    step1 = Step()
    j1 = step1.add_job(func, "a", b="b")
    step2 = step1.create_next_step()
    j2 = step2.add_job(funcs)

    run_job(j1)

    assert JobDBModel.objects.get(id=j1.id).status == JobDBModel.Status.DONE
    assert JobDBModel.objects.get(id=j2.id).status == JobDBModel.Status.NEW

    run_job(JobDBModel.objects.get(id=j2.id))

    assert JobDBModel.objects.get(id=j2.id).status == JobDBModel.Status.DONE


def test_four_steps_with_one_step_with_multiple_jobs(db):
    step1 = Step()
    j1 = step1.add_job(func, "a", b="b")

    step2 = step1.create_next_step()
    j2 = step2.add_job(funcs)

    step3 = step2.create_next_step()
    j3 = step3.add_job(func, "a", b="b")
    j4 = step3.add_job(func, "a", b="b")
    j5 = step3.add_job(func, "a", b="b")

    step4 = step3.create_next_step()
    j6 = step4.add_job(funcs)

    assert JobDBModel.objects.get(id=j1.id).status == JobDBModel.Status.NEW
    assert JobDBModel.objects.get(id=j2.id).status == JobDBModel.Status.BLOCKED
    assert JobDBModel.objects.get(id=j3.id).status == JobDBModel.Status.BLOCKED
    assert JobDBModel.objects.get(id=j4.id).status == JobDBModel.Status.BLOCKED
    assert JobDBModel.objects.get(id=j5.id).status == JobDBModel.Status.BLOCKED
    assert JobDBModel.objects.get(id=j6.id).status == JobDBModel.Status.BLOCKED

    assert JobDBModel.objects.get(id=j1.id).next_step == step2.id
    assert JobDBModel.objects.get(id=j2.id).next_step == step3.id
    assert JobDBModel.objects.get(id=j3.id).next_step == step4.id
    assert JobDBModel.objects.get(id=j4.id).next_step == step4.id
    assert JobDBModel.objects.get(id=j5.id).next_step == step4.id
    assert JobDBModel.objects.get(id=j6.id).next_step is None


def test_running_job_not_in_new_status(db):
    step1 = Step()
    j1 = step1.add_job(func, "a", b="b")

    step2 = step1.create_next_step()
    j2 = step2.add_job(funcs)

    with pytest.raises(ValueError):
        run_job(j2)


def test_run_four_steps_with_one_step_with_multiple_jobs(db):
    step1 = Step()
    j1 = step1.add_job(func, "a", b="b")

    step2 = step1.create_next_step()
    j2 = step2.add_job(funcs)

    step3 = step2.create_next_step()
    j3 = step3.add_job(func, "a", b="b")
    j4 = step3.add_job(func, "a", b="b")
    j5 = step3.add_job(func, "a", b="b")

    step4 = step3.create_next_step()
    j6 = step4.add_job(funcs)

    run_job(j1)
    run_job(JobDBModel.objects.get(id=j2.id))

    assert JobDBModel.objects.get(id=j3.id).status == JobDBModel.Status.NEW
    assert JobDBModel.objects.get(id=j4.id).status == JobDBModel.Status.NEW
    assert JobDBModel.objects.get(id=j5.id).status == JobDBModel.Status.NEW
    assert JobDBModel.objects.get(id=j6.id).status == JobDBModel.Status.BLOCKED
