import re
import subprocess
import tempfile
import time
import Utils, JobManager, Email, Config

from time import strftime, localtime
from threading import Thread, Event

class Job:
    pass

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

        while True:
            if not Utils.is_proxy_valid():
                Email.sendProxy(email, self.folder)
                Utils.delegate_proxy(self.verbose)

            self.status()

            get_id = []
            kill_id = []
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
                print("[%s] Going to sleep for 30 minutes" % strftime("%H:%M:%S", localtime()))

            # Wait for 10 minutes
            self.exit.wait(30 * 60)
            if self.exit.is_set():
                interrupted = True
                break

            print("-----------------------------")
            print("")

        if not interrupted:
            Email.sendComplete(email, self.folder, self.jobs)

    def status(self):
        """Execute crab -status on the specified folder"""

        if self.verbose:
            print("Executing crab -status on folder '%s'" % self.folder)

        cmdline = "crab -status -c %s" % (self.folder)
        (output, returncode) = Utils.runCommand(cmdline)

        if returncode != 0:
            raise IOError("Unable to get status from crab. Output is:\n%s" % "\n".join(output))

        self.crab_status = output

        #self.crab_status = """crab:  Version 2.10.2 running on Sun Dec 22 12:44:52 2013 CET (11:44:52 UTC)

#crab. Working options:
    #scheduler           remoteGlideIn
    #job type            CMSSW
    #server              OFF
    #working directory   /afs/cern.ch/user/s/sbrochet/HTT/SLC5/CMSSW_5_3_12_patch3/src/FullsimFramework/STEP_1/crab_S0_S_i_M700_cpl1_pseudoscalar/

#crab:  Checking the status of all jobs: please wait
#crab:  contacting remote host submit-6.t2.ucsd.edu
#crab:  
#ID    END STATUS            ACTION       ExeExitCode JobExitCode E_HOST
#----- --- ----------------- ------------  ---------- ----------- ---------
#1     N   Running           SubSuccess                           lyogrid07.in2p3.fr
#2     N   Running           SubSuccess                           lyogrid07.in2p3.fr
#3     N   Running           SubSuccess                           lyogrid07.in2p3.fr
#4     N   Running           SubSuccess                           ce203.cern.ch
#5     N   Running           SubSuccess                           ce203.cern.ch
#6     N   Running           SubSuccess                           ce203.cern.ch
#7     N   Running           SubSuccess                           ce203.cern.ch
#8     N   Running           SubSuccess                           ce203.cern.ch
#9     N   Running           SubSuccess                           lyogrid07.in2p3.fr
#10    N   Running           SubSuccess                           lyogrid07.in2p3.fr
#--------------------------------------------------------------------------------
#11    N   Running           SubSuccess                           lyogrid07.in2p3.fr
#12    N   Running           SubSuccess                           lyogrid07.in2p3.fr
#13    N   Running           SubSuccess                           llrcream.in2p3.fr
#14    N   Running           SubSuccess                           llrcream.in2p3.fr
#15    N   Running           SubSuccess                           ce203.cern.ch
#16    N   Running           SubSuccess                           ce203.cern.ch
#17    N   Running           SubSuccess                           ce203.cern.ch
#18    N   Running           SubSuccess                           ce203.cern.ch
#19    N   Running           SubSuccess                           ce203.cern.ch
#20    N   Running           SubSuccess                           ce203.cern.ch
#--------------------------------------------------------------------------------
#21    N   Running           SubSuccess                           ce203.cern.ch
#22    N   Running           SubSuccess                           ce203.cern.ch
#23    N   Running           SubSuccess                           ce203.cern.ch
#24    N   Running           SubSuccess                           ce203.cern.ch
#25    N   Running           SubSuccess                           ce203.cern.ch
#26    N   Running           SubSuccess                           ce203.cern.ch
#27    N   Running           SubSuccess                           ce203.cern.ch
#28    N   Running           SubSuccess                           ce203.cern.ch
#29    N   Running           SubSuccess                           ce203.cern.ch
#30    N   Running           SubSuccess                           llrcream.in2p3.fr
#--------------------------------------------------------------------------------
#31    N   Running           SubSuccess                           llrcream.in2p3.fr
#32    N   Running           SubSuccess                           llrcream.in2p3.fr
#33    N   Running           SubSuccess                           llrcream.in2p3.fr
#34    N   Running           SubSuccess                           llrcream.in2p3.fr
#35    N   Running           SubSuccess                           llrcream.in2p3.fr
#36    N   Running           SubSuccess                           ce203.cern.ch
#37    N   Running           SubSuccess                           ce203.cern.ch
#38    N   Running           SubSuccess                           ce203.cern.ch
#39    N   Running           SubSuccess                           ce203.cern.ch
#40    N   Running           SubSuccess                           ce203.cern.ch
#--------------------------------------------------------------------------------
#41    N   Running           SubSuccess                           ce203.cern.ch
#42    N   Running           SubSuccess                           ce203.cern.ch
#43    N   Running           SubSuccess                           ce203.cern.ch
#44    N   Running           SubSuccess                           ce203.cern.ch
#45    N   Running           SubSuccess                           ce203.cern.ch
#46    N   Running           SubSuccess                           ce203.cern.ch
#47    N   Running           SubSuccess                           lyogrid07.in2p3.fr
#48    N   Running           SubSuccess                           lyogrid07.in2p3.fr
#49    N   Running           SubSuccess                           lyogrid07.in2p3.fr
#50    N   Running           SubSuccess                           lyogrid07.in2p3.fr
#--------------------------------------------------------------------------------
#51    N   Running           SubSuccess                           lyogrid07.in2p3.fr
#52    N   Running           SubSuccess                           lyogrid07.in2p3.fr
#53    N   Running           SubSuccess                           lyogrid07.in2p3.fr
#54    N   Running           SubSuccess                           lyogrid07.in2p3.fr
#55    N   Running           SubSuccess                           lyogrid07.in2p3.fr
#56    N   Running           SubSuccess                           lyogrid07.in2p3.fr
#57    N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#58    N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#59    N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#60    N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#--------------------------------------------------------------------------------
#61    N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#62    N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#63    N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#64    N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#65    N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#66    N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#67    N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#68    N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#69    N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#70    N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#--------------------------------------------------------------------------------
#71    N   Aborted           SubSuccess                           llrcream.in2p3.fr
#72    N   Aborted           SubSuccess                           llrcream.in2p3.fr
#73    N   Aborted           SubSuccess                           llrcream.in2p3.fr
#74    N   Aborted           SubSuccess                           ce203.cern.ch
#75    N   Aborted           SubSuccess                           ce203.cern.ch
#76    N   Aborted           SubSuccess                           ce203.cern.ch
#77    N   Aborted           SubSuccess                           ce203.cern.ch
#78    N   Aborted           SubSuccess                           ce203.cern.ch
#79    N   Aborted           SubSuccess                           ce203.cern.ch
#80    N   Aborted           SubSuccess                           ce203.cern.ch
#--------------------------------------------------------------------------------
#81    N   Aborted           SubSuccess                           ce203.cern.ch
#82    N   Aborted           SubSuccess                           ce203.cern.ch
#83    N   Aborted           SubSuccess                           ce203.cern.ch
#84    N   Aborted           SubSuccess                           ce203.cern.ch
#85    N   Aborted           SubSuccess                           ce203.cern.ch
#86    N   Aborted           SubSuccess                           ce203.cern.ch
#87    N   Aborted           SubSuccess                           ce203.cern.ch
#88    N   Aborted           SubSuccess                           ce203.cern.ch
#89    N   Aborted           SubSuccess                           ce203.cern.ch
#90    N   Aborted           SubSuccess                           ce203.cern.ch
#--------------------------------------------------------------------------------
#91    N   Aborted           SubSuccess                           ce203.cern.ch
#92    N   Aborted           SubSuccess                           ce203.cern.ch
#93    N   Aborted           SubSuccess                           ce203.cern.ch
#94    N   Aborted           SubSuccess                           ce203.cern.ch
#95    N   Aborted           SubSuccess                           ce203.cern.ch
#96    N   Aborted           SubSuccess                           ce203.cern.ch
#97    N   Aborted           SubSuccess                           ce203.cern.ch
#98    N   Aborted           SubSuccess                           ce203.cern.ch
#99    N   Aborted           SubSuccess                           ce203.cern.ch
#100   N   Aborted           SubSuccess                           ce203.cern.ch
#--------------------------------------------------------------------------------
#101   N   Aborted           SubSuccess                           ce203.cern.ch
#102   N   Aborted           SubSuccess                           llrcream.in2p3.fr
#103   N   Aborted           SubSuccess                           llrcream.in2p3.fr
#104   N   Aborted           SubSuccess                           llrcream.in2p3.fr
#105   N   Aborted           SubSuccess                           llrcream.in2p3.fr
#106   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#107   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#108   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#109   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#110   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#--------------------------------------------------------------------------------
#111   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#112   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#113   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#114   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#115   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#116   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#117   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#118   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#119   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#120   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#--------------------------------------------------------------------------------
#121   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#122   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#123   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#124   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#125   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#126   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#127   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#128   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#129   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#130   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#--------------------------------------------------------------------------------
#131   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#132   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#133   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#134   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#135   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#136   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#137   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#138   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#139   N   Aborted           SubSuccess                           llrcream.in2p3.fr
#140   N   Aborted           SubSuccess                           ce203.cern.ch
#--------------------------------------------------------------------------------
#141   N   Aborted           SubSuccess                           ce203.cern.ch
#142   N   Aborted           SubSuccess                           ce203.cern.ch
#143   N   Aborted           SubSuccess                           ce203.cern.ch
#144   N   Aborted           SubSuccess                           ce203.cern.ch
#145   N   Aborted           SubSuccess                           ce203.cern.ch
#146   N   Aborted           SubSuccess                           ce203.cern.ch
#147   N   Aborted           SubSuccess                           ce203.cern.ch
#148   N   Aborted           SubSuccess                           ce203.cern.ch
#149   N   Aborted           SubSuccess                           ce203.cern.ch
#150   N   Aborted           SubSuccess                           ce203.cern.ch
#--------------------------------------------------------------------------------
#151   N   Aborted           SubSuccess                           ce203.cern.ch
#152   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#153   N   Aborted           SubSuccess                           llrcream.in2p3.fr
#154   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#155   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#156   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#157   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#158   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#159   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#160   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#--------------------------------------------------------------------------------
#161   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#162   N   Aborted           SubSuccess                           ce203.cern.ch
#163   N   Aborted           SubSuccess                           ce203.cern.ch
#164   N   Aborted           SubSuccess                           ce203.cern.ch
#165   N   Aborted           SubSuccess                           llrcream.in2p3.fr
#166   N   Aborted           SubSuccess                           llrcream.in2p3.fr
#167   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#168   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#169   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#170   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#--------------------------------------------------------------------------------
#171   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#172   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#173   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#174   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#175   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#176   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#177   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#178   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#179   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#180   N   Aborted           SubSuccess                           ce203.cern.ch
#--------------------------------------------------------------------------------
#181   N   Aborted           SubSuccess                           ce203.cern.ch
#182   N   Aborted           SubSuccess                           llrcream.in2p3.fr
#183   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#184   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#185   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#186   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#187   N   Aborted           SubSuccess                           llrcream.in2p3.fr
#188   N   Aborted           SubSuccess                           lyogrid07.in2p3.fr
#189   N   Aborted           SubSuccess                           ce203.cern.ch
#190   N   Aborted           SubSuccess                           ce203.cern.ch
#--------------------------------------------------------------------------------
#191   N   Aborted           SubSuccess                           ce203.cern.ch
#192   N   Aborted           SubSuccess                           ce203.cern.ch
#3313  Y   Retrieved         Cleared       0          0           ce203.cern.ch
#3314  Y   Retrieved         Cleared       0          0           ce203.cern.ch
#3315  Y   Retrieved         Cleared       8021       8021        llrcream.in2p3.fr
#3316  Y   Retrieved         Cleared       0          0           llrcream.in2p3.fr
#1222  N   Done              Terminated    8020       8020        llrcream.in2p3.fr
#1223  Y   Retrieved         Cleared       0          0           ce203.cern.ch""".split("\n")
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
