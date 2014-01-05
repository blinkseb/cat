import subprocess, tempfile

import Config

def runCrab(*args):
    if len(args) == 2:
        args = ["crab", "-%s" % args[0], "-c", args[1]]
    else:
        args = ["crab", "-%s" % args[0], args[1], "-c", args[2]]

    return runCommand(" ".join(args))

def runCommand(cmdline):
    tmp = tempfile.NamedTemporaryFile(dir = "/tmp")
    cmdline = "%s &> %s" % (cmdline, tmp.name)

    p = subprocess.Popen(cmdline, shell = True)

    p.communicate()

    output = tmp.readlines()

    tmp.close()

    return (output, p.returncode)

def is_number(string):
    try:
        int(string)
        return True
    except ValueError:
        return False

def delegate_proxy(verbose, email = True):
    """Run voms-proxy-init in order to delegate a fresh new proxy valid for 8 days"""

    password = Config.get().get()["grid_password"] if "grid_password" in Config.get().get() else None

    if password is None:
        if email:
            Email.sendProxy(email, self.folder)
        import getpass
        password = getpass.getpass("Enter your grid certificate password: ")

    args = ["voms-proxy-init", "-voms", "cms", "-pwstdin", "-valid", "192:00"]
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE, bufsize = -1)

    (stdout, stderr) = p.communicate(password + "\n")
    if p.returncode != 0:
        raise IOError("Unable to delegate proxy.")
    
    if (verbose):
        print stdout
        print stderr

def is_proxy_valid():
    """
    Check if the current proxy is valid

    To be valid, the proxy must have at least 170 hours remaining (7 days and 2 hours)

    """

    args = ["voms-proxy-info", "-exists", "-valid", "170:0"]
    p = subprocess.Popen(args, stderr = subprocess.PIPE, stdout = subprocess.PIPE)
    p.communicate()

    return p.returncode == 0
