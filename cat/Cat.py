import os
import signal
import time
import CrabMonitor, Utils

from optparse import OptionParser


class Cat:
    """Main class of Cat"""

    def __init__(self):
        parser = OptionParser()
        parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False, help="Print status messages to stdout")
        parser.add_option("-d", "--dry-run", action="store_true", dest="dry_run", default=False, help="Print what actions will be executed, but don't execute anything")

        (self.options, self.args) = parser.parse_args()

        if len(self.args) != 1:
            parser.error("Please specify the crab task to monitor")

        self.crab_folder = self.args[0]
        if not os.path.isdir(self.crab_folder):
            parser.error("The folder '%s' does not exist or is not a valid folder" % self.crab_folder)

        self.monitor = None
        # Register SIGINT handler
        signal.signal(signal.SIGINT, self.signal_handler)

    def run(self):
        # Delegate a new proxy
        if not Utils.is_proxy_valid():
            Utils.delegate_proxy(self.options.verbose)

        # Create monitor thread
        self.monitor = CrabMonitor.CrabMonitor(self.crab_folder, self.options.verbose, self.options.dry_run)
        self.monitor.start()
        while self.monitor.isAlive():
            self.monitor.join(0.5)

    def signal_handler(self, signum, frame):
        if self.monitor is not None:
            self.monitor.exit.set()
