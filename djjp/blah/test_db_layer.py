from django_async_job_pipelines.db_layer import db as djjp_db
from django.utils import timezone
from datetime import timedelta
from django_async_job_pipelines.models import Manager


def test_send_manager_beat(db):
    djjp_db.get_or_create_manager()

    djjp_db.update_manager_beat(timezone.now() - timedelta(days=10))
    manager = djjp_db.get_or_create_manager()

    assert manager.updated_at < timezone.now() - timedelta(days=9)

    djjp_db.send_manager_beat()
    manager = djjp_db.get_or_create_manager()

    assert manager.updated_at > timezone.now() - timedelta(seconds=1)
