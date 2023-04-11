from datetime import datetime
from rq_scheduler import Scheduler
from redis import Redis

from project_monitor import task

scheduler = Scheduler(connection=Redis())
def main():
    scheduler.schedule(
        scheduled_time=datetime.utcnow(), # Time for first execution, in UTC timezone
        func=task,                     # Function to be queued
        args=[],             # Arguments passed into function when executed
        kwargs={},         # Keyword arguments passed into function when executed
        interval=120,                   # Time before the function is called again, in seconds
        repeat=None,                     # Repeat this number of times (None means repeat forever)
        meta={}            # Arbitrary pickleable data on the job itself
    )

if __name__ == "__main__":
    main()