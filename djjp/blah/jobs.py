from django_async_job_pipelines.jobs import job, djjp_currently_running_job
import asyncio
from time import sleep
from rich import print
import random

from django_async_job_pipelines.db_layer import db
from .models import JobResult


@job(name="name", timeout=10)
def func(a, b):
    choices = [0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1]
    """
    function implementation
    """
    choice = random.choice(choices)
    sleep(choice)
    JobResult.objects.using(db.implementation.name).create(
        result="async job", job=djjp_currently_running_job.get()
    )
    print("[yellow]Slept: [/yellow]", choice)


@job(name="async_job", timeout=10)
async def afunc(a, b):
    choices = [0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1]
    """
    function implementation
    """
    await JobResult.objects.using(db.implementation.name).acreate(
        result="async job", job=djjp_currently_running_job.get()
    )
    choice = random.choice(choices)
    await asyncio.sleep(choice)
    print("[yellow]Slept: [/yellow]", choice)
