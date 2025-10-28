import djclick as click
from rich import print

from django_async_job_pipelines.db_layer import db
from django_async_job_pipelines.models import JobDBModel, LockedJob
from blah.models import JobResult


@click.command()
@click.argument("num-done", type=int)
@click.argument("num-error", type=int, default=0)
@click.argument("num-results", type=int, default=0)
def command(num_done: int, num_error: int, num_results: int):
    actually_done = (
        JobDBModel.objects.using(db.implementation.name)
        .filter(status=JobDBModel.Status.DONE)
        .count()
    )
    new_count = (
        JobDBModel.objects.using(db.implementation.name)
        .filter(status=JobDBModel.Status.NEW)
        .count()
    )
    error_count = (
        JobDBModel.objects.using(db.implementation.name)
        .filter(status=JobDBModel.Status.ERROR)
        .count()
    )
    first = JobDBModel.objects.using(db.implementation.name).first()

    assert actually_done == num_done, actually_done
    print(f"[green]Checked {num_done} background jobs are DONE in db[/green]")

    assert error_count == num_error, error_count
    print(f"[green]Checked {num_error} background jobs are ERROR in db[/green]")

    assert LockedJob.objects.using(db.implementation.name).count() == 0, (
        LockedJob.objects.using(db.implementation.name).count()
    )
    print("[green]Checked there are no locked jobs in db[/green]")

    assert JobResult.objects.using(db.implementation.name).count() == num_results, (
        JobResult.objects.using(db.implementation.name).count()
    )
    print(f"[green]Checked {num_results} job results are in db[/green]")

    for j in JobDBModel.objects.using(db.implementation.name).all():
        assert j.manager_id
    print("[green]Checked all jobs have manager_id[/green]")
