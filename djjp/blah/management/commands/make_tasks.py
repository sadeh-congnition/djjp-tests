import djclick as click
from rich import print

from django_async_job_pipelines.jobs import job
from django_async_job_pipelines.models import JobDBModel, LockedJob
from blah.jobs import func, afunc
from blah.models import JobResult
from django_async_job_pipelines.db_layer import db


@click.command()
@click.argument("num_jobs", default=1)
def command(num_jobs: int):
    JobDBModel.objects.using(db.implementation.name).all().delete()
    LockedJob.objects.using(db.implementation.name).all().delete()
    JobResult.objects.using(db.implementation.name).all().delete()

    num_jobs = int(num_jobs)
    for i in range(num_jobs):
        func.run_later("a", b="b")

    assert JobDBModel.objects.using(db.implementation.name).count() == num_jobs

    print(f"[green]Created {num_jobs} background jobs in db[/green]")
