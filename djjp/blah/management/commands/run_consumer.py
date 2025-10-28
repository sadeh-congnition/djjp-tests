from django.core.management.base import BaseCommand
from django.core.management import call_command

from blah.jobs import *  # registers jobs


class Command(BaseCommand):
    def handle(self, *args, **options):
        call_command("start_consumer")
