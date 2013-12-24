class JobStatus:
    def __init__(self, job):
        self.job = job

    def done(self):
        return False

    def killable(self):
        return False

    def gettable(self):
        return False

    def submittable(self):
        return False

    def success(self):
        return self.job.job_exit_code == 0 and self.job.grid_exit_code == 0

    def failed(self):
        return self.done() and not self.success()

    def running(self):
        return False

    def waiting(self):
        return False

class DoneStatus(JobStatus):
    def __init__(self, job):
        JobStatus.__init__(self, job)

    def done(self):
        return True

    def gettable(self):
        return True

class RetrievedStatus(JobStatus):
    def __init__(self, job):
        JobStatus.__init__(self, job)

    def done(self):
        return True

class SubmittedStatus(JobStatus):
    def __init__(self, job):
        JobStatus.__init__(self, job)

    def waiting(self):
        return True

class RunningStatus(JobStatus):
    def __init__(self, job):
        JobStatus.__init__(self, job)

    def running(self):
        return True

class AbortedStatus(JobStatus):
    def __init__(self, job):
        JobStatus.__init__(self, job)

    def done(self):
        return True

class CancelledStatus(JobStatus):
    def __init__(self, job):
        JobStatus.__init__(self, job)

    def done(self):
        return True

    def killable(self):
        return True

class CreatedStatus(JobStatus):
    def __init__(self, job):
        JobStatus.__init__(self, job)

    def submittable(self):
        return True

def create(job):
    """Create a JobStatus object based on current job status"""
    status = job.status
    return globals()["%sStatus" % status](job)
