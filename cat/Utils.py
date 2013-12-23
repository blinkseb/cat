import subprocess

def run(args):
    """Run the process using args and return the output"""

    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize = -1)

    (stdout, stderr) = p.communicate()
    return (stdout, p.returncode)

def runCrab(*args):
    if len(args) == 2:
        args = ["crab", "-%s" % args[0], "-c", args[1]]
    else:
        args = ["crab", "-%s" % args[0], args[1], "-c", args[2]]

    return run(args)

def is_number(string):
    try:
        int(string)
        return True
    except ValueError:
        return False

def delegate_proxy(verbose):
    """Run voms-proxy-init in order to delegate a fresh new proxy"""
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

    To be valid, the proxy must have at least 112 hours remaining

    """

    args = ["voms-proxy-info", "-exists", "-valid", "112:0"]
    p = subprocess.Popen(args, stderr = subprocess.PIPE, stdout = subprocess.PIPE)
    p.communicate()

    return p.returncode == 0
