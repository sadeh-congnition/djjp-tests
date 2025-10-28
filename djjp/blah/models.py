from django.db import models
from django_async_job_pipelines.models import JobDBModel


class JobResult(models.Model):
    result = models.TextField(null=True, blank=True)
    job = models.ForeignKey(JobDBModel, on_delete=models.CASCADE)
