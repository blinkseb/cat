import re
import subprocess
import tempfile
import time
import Utils, JobManager, Email, Config

from time import strftime, localtime
from threading import Thread, Event

class Job:
    pass

def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

class CrabMonitor(Thread):
    def __init__(self, folder, verbose = False, dry_run = False):
        Thread.__init__(self, name = "Crab monitor thread")

        self.folder = folder
        self.verbose = verbose
        self.dry_run = dry_run

        self.exit = Event()

    def run(self):
        interrupted = False
        corrupted_job_regex = re.compile("Output files for job (\d+) seems corrupted")
        email = Config.get().get()["email"]

        try:
            while True:
                if not Utils.is_proxy_valid():
                    Email.sendProxy(email, self.folder)
                    Utils.delegate_proxy(self.verbose)

                self.status()

                get_id = []
                kill_id = []
                submit_id = []
                resubmit_id = []
                force_resubmit_id = []
                corrupted_id = []
                
                n_waiting = 0
                n_running = 0

                for (id, job) in self.jobs.items():
                    status = JobManager.create(job)

                    if status.gettable():
                        get_id.append(str(id))

                    if status.killable():
                        kill_id.append(str(id))

                    if status.submittable():
                        submit_id.append(str(id))

                    if status.failed():
                        resubmit_id.append(str(id))

                    if status.running():
                        n_running = n_running + 1

                    if status.waiting():
                        n_waiting = n_waiting + 1

                    job._status = status

                if self.verbose:
                    if len(get_id) > 0:
                        print("I'll get jobs " + ",".join(get_id))
                    else:
                        print("No job to get output for")

                    if len(kill_id) > 0:
                        print("I'll kill jobs " + ",".join(kill_id))
                    else:
                        print("No job to kill")

                    if len(resubmit_id) > 0:
                        print("I'll resubmit jobs " + ",".join(resubmit_id))
                    else:
                        print("No job to resubmit")

                    print("")
                    self.dump()

                log = ""
                if not self.dry_run:
                    if len(get_id) > 0:
                        if self.verbose:
                            print("Retrieving jobs...")
                        (output, returncode) = Utils.runCrab("get", ",".join(get_id), self.folder)
                        log += "crab -get output:\n"
                        log += "".join(output)
                        log += "\n"

                        # Detect corrupted jobs
                        lines = output
                        for line in lines:
                            matches = re.search(corrupted_job_regex, line)
                            if matches is not None:
                                corrupted_id.append(str(matches.group(1)))

                    if len(corrupted_id) > 0:
                        if self.verbose:
                            print("Some jobs are corrupted: " + ",".join(corrupted_id))
                        kill_id.extend(corrupted_id)
                        kill_id.sort()
                        force_resubmit_id.extend(corrupted_id)
                        force_resubmit_id.sort()

                    if len(kill_id) > 0:
                        if self.verbose:
                            print("Killing jobs...")
                        (output, returncode) = Utils.runCrab("kill", ",".join(kill_id), self.folder)
                        log += "crab -kill output:\n"
                        log += "".join(output)
                        log += "\n"

                    if len(submit_id) > 0:
                        # Crab only accept a maximum of 500 jobs on submit.
                        splitted_submit_ids = chunks(submit_id, 500)
                        for splitted_submit_id in splitted_submit_ids:
                            if self.verbose:
                                print("Submitting jobs...")
                            (output, returncode) = Utils.runCrab("submit", ",".join(splitted_submit_id), self.folder)
                            log += "crab -submit output:\n"
                            log += "".join(output)
                            log += "\n"


                    if len(resubmit_id) > 0:
                        if self.verbose:
                            print("Resubmitting jobs...")
                        (output, returncode) = Utils.runCrab("resubmit", ",".join(resubmit_id), self.folder)
                        log += "crab -resubmit output:\n"
                        log += "".join(output)
                        log += "\n"
                    
                    if len(force_resubmit_id) > 0:
                        if self.verbose:
                            print("Force-resubmitting jobs...")
                        (output, returnCode) = Utils.runCrab("forceResubmit", ",".join(force_resubmit_id), self.folder)
                        log += "crab -forceResubmit output:\n"
                        log += "".join(output)
                        log += "\n"

                    print("\nAll actions done")
                    print("")

                Email.sendReport(email, self.folder, get_id, kill_id, resubmit_id, force_resubmit_id, corrupted_id, log, self.jobs)

                if len(resubmit_id) == 0 and n_running == 0 and n_waiting == 0:
                    break
                
                if self.verbose:
                    print("[%s] Going to sleep for 2 hours" % strftime("%H:%M:%S", localtime()))

                # Wait for 2 hours
                self.exit.wait(2 * 60 * 60)
                if self.exit.is_set():
                    interrupted = True
                    break

                print("-----------------------------")
                print("")

            if not interrupted:
                Email.sendComplete(email, self.folder, self.jobs)
        except:
            # Send an email on exception
            Email.sendCrash(email, self.folder)
            raise

    def status(self):
        """Execute crab -status on the specified folder"""

        if self.verbose:
            print("Executing crab -status on folder '%s'" % self.folder)

        cmdline = "crab -status -c %s" % (self.folder)
        (output, returncode) = Utils.runCommand(cmdline)

        if returncode != 0:
            raise IOError("Unable to get status from crab. Output is:\n%s" % "\n".join(output))

        self.crab_status = output
        self._parse()

    def _parse(self):
        """Parse the content of crab status"""

        if self.verbose:
            print("Parsing jobs")

        lines = self.crab_status

        self.crab_summary = ""
        summary = False
        jobs = {}
        for line in lines:
            if "ExitCodes Summary" in line:
                summary = True

            if summary:
                self.crab_summary += line
                continue

            data = line.split()
            if len(data) < 2 or not Utils.is_number(data[0]):
                continue

            job = Job()
            job.id = int(data[0])
            job.done = True if data[1] == "Y" else False
            job.status = data[2]
            job.grid_status = data[3]

            if len(data) == 7:
                job.job_exit_code = int(data[4])
                job.grid_exit_code = int(data[5])
                job.computing_element = data[6]
            elif len(data) == 5:
                job.job_exit_code = -1
                job.grid_exit_code = -1
                job.computing_element = data[4]
            elif len(data) == 6:
                job.job_exit_code = -1
                job.grid_exit_code = int(data[4])
                job.computing_element = data[5]
            else:
                job.job_exit_code = -1
                job.grid_exit_code = -1
                job.computing_element = None
          

            jobs[job.id] = job

        self.jobs = jobs
        if self.verbose:
            print("%d jobs parsed" % len(self.jobs))
        
    def dump(self):
       print(self.crab_summary)
       #for id, job in self.jobs.items():
           #print("%d @ %s -> %s:%s" % (id, job.computing_element, job.status, job.grid_status))
