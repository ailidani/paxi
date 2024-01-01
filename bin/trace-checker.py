import os
import argparse

import lineparse
import system_state

def parse_args():
    parser = argparse.ArgumentParser(description="Parser")

    parser.add_argument("logfiles", type=argparse.FileType("r"),
                        nargs="+",
                        help="Add log files")

    def is_file(arg):
        # check if the file exists
        try:
            with open(arg, "r"):
                return arg
        except OSError as e:
            raise argparse.ArgumentTypeError(e)

    parser.add_argument("specfile", type=is_file,
                        help="The TLA+ spec you want to check against")

    parser.add_argument(
        "--tla2tools-jar", help="Path to the jar file, else it would be downloaded")

    return parser.parse_args()


def update_state(current_state, log_event):
    """ current_state is a LogEvent """


class TLCInputs:
    def __init__(self) -> None:
        self.dir_path = None
        self.spec = None
        self.config = None

    def __enter__(self):
        spec_filename = "Trace.tla"
        cfg_filename = "Trace.cfg"
        mode="w+"

        self.dir_path = os.getcwd()
        self.spec = open(spec_filename, mode)
        self.config = open(cfg_filename, mode)

        return self

    def __exit__(self):
        self.spec.close()
        self.config.close()
        
