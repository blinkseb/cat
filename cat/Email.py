import re
import smtplib
import socket

from email.mime.text import MIMEText
from string import Template

def send(to, msg):
    me = "CAT <no-reply@ipnl.in2p3.fr"
    msg["From"] = me

    s = smtplib.SMTP('localhost')
    s.sendmail(me, [to], msg.as_string())
    s.quit()

MAIL_REGEX = re.compile("(.{76})", re.DOTALL)
def sendReport(to, folder, get_ids, kill_ids, resubmit_ids, corrupted_ids, jobs):
    template = Template("""CAT has finish its duty. Here's a summary of the actions taken:

    $n_get jobs got: $get_id
    $n_kill jobs killed: $kill_id
    $n_resubmit jobs resubmitted: $resubmit_id
    $n_corrupted corrupted jobs: $corrupted_id

$n_running jobs are still running, while $n_submitted are waiting.

Total of jobs in this task: $n_jobs

--
CAT currently running on $host""")

    args = {}
    args["n_get"] = len(get_ids)
    args["get_id"] = ",".join(get_ids)

    args["n_kill"] = len(kill_ids)
    args["kill_id"] = ",".join(kill_ids)

    args["n_resubmit"] = len(resubmit_ids)
    args["resubmit_id"] = ",".join(resubmit_ids)

    args["n_corrupted"] = len(corrupted_ids)
    args["corrupted_id"] = ",".join(corrupted_ids)

    n_running = 0
    n_waiting = 0

    for id, job in jobs.items():
        if job._status.running():
            n_running = n_running + 1

        if job._status.waiting():
            n_waiting = n_waiting + 1

    args["n_running"] = n_running
    args["n_submitted"] = n_waiting
    args["n_jobs"] = len(jobs)

    args["host"] = socket.gethostname()

    s = template.substitute(args)
    formatted_message = []
    for line in s.split("\n"):
        line = MAIL_REGEX.sub("\\1\n", line, 0)
        formatted_message.append(line)

    msg = MIMEText("\n".join(formatted_message))
    msg["Subject"] = "[CAT] Report for task %s" % folder
    msg["To"] = to

    send(to, msg)

def sendComplete(to, folder, jobs):
    m = "CAT has finish its duty. %d jobs done, and CAT will exit." % len(jobs)

    msg = MIMEText(m)
    msg["Subject"] = "[CAT] Task %s is done" % folder
    msg["To"] = to

    send(to, msg)

def sendProxy(to, folder):
    m = "Your proxy is about to expire. CAT is waiting for your password on host %s" % socket.gethostname()

    msg = MIMEText(m)
    msg["Subject"] = "[CAT] Please renew proxy for task %s" % folder
    msg["To"] = to

    send(to, msg)
