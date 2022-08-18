import docopt
import sys
import importlib

USAGE = {}
USAGE[
    "main"
] = """Access Progeny CLI tools.

Usage:
  progeny [-h] [serve]

Subcommands:
  serve                 Start a middleware server for Prodigy instances.

Optional arguments:
  -h --help             Show this
"""

USAGE[
    "serve"
] = """Start a middleware server for Prodigy instances.

Usage:
  progeny serve [-h] [--port <port>]

Details:
  The prodigy instances accessible must have been started *before* running
  this command.

Optional arguments:
  -p --port=<port>      Set port number [default: 9000].
  -h --help             Show this
"""
SUBCOMMANDS = [sub for sub in USAGE if sub != "main"]


def parse_main():
    modified_argv = [arg for arg in sys.argv[1:] if arg in SUBCOMMANDS + ["--help", "-h"]]
    return docopt.docopt(USAGE["main"], argv=modified_argv, help=False)


def run(subcommand, args):
    m = importlib.import_module(f"progeny_core.cli.cli_{subcommand}")
    args.pop(subcommand)
    m.run(args)


def main():
    args = parse_main()
    subcommand = "main"
    for sub in SUBCOMMANDS:
        if args[sub]:
            args = docopt.docopt(USAGE[sub], help=True)
            subcommand = sub
            break
    else:
        print(USAGE["main"])
        sys.exit()

    run(subcommand, args)
