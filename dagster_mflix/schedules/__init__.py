from dagster import ScheduleDefinition
from ..jobs import movies_job


movies_schedule = ScheduleDefinition(
    job=movies_job,
    cron_schedule="* * * * *", # Run every minute, demo purposes only
)
