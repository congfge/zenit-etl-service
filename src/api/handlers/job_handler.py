from services.redis_job_tracker import RedisJobTracker

def list_recent_jobs(limit: int = 10):
    tracker = RedisJobTracker()
    jobs = tracker.list_jobs(limit=limit)
    return jobs

def get_job(job_id: str):
    tracker = RedisJobTracker()
    job = tracker.get_job(job_id)
    return job