from django.contrib import admin
from django_async_job_pipelines.admin import MultiDBModelAdmin
from .models import JobResult


@admin.register(JobResult)
class JobResultAdmin(MultiDBModelAdmin):
    list_display = ["result", "job"]
