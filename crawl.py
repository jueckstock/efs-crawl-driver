#!/usr/bin/env python3
import os
import sys
import subprocess
import hashlib
import random
import re
import string
import threading
from urllib.parse import urlparse

REPLACEX = re.compile(r"[^-_a-zA-Z0-9]")

TAG = os.environ.get("TAG", 'tag')
PROFILE = os.environ.get("PROFILE", False)
TIME_LIMIT = float(os.environ.get("TIME_LIMIT", 15.0))
TIME_OUT = float(os.environ.get("TIME_OUT", max(TIME_LIMIT, 1.0) * 4))
TIME_TO_KILL = float(os.environ.get("TIME_TO_KILL", 5.0))
BROWSER_EXE = os.environ.get("BROWSER_EXE", "/home/jjuecks/brave/Static/brave")
NPM_CWD = os.environ.get("NPM_CWD", "/home/jjuecks/brave/pagegraph-crawl")


def run_with_timeout(cmd_argv, **cmd_options):
    time_out_limit = cmd_options.pop("TIME_OUT", 60.0)
    time_to_kill_limit = cmd_options.pop("TIME_TO_KILL", 5.0)

    status = None
    with subprocess.Popen(cmd_argv, **cmd_options) as proc:
        try:
            status = proc.wait(timeout=time_out_limit)
            if status != 0:
                print(f"ERROR: status={status}", flush=True)
        except subprocess.TimeoutExpired:
            proc.terminate() # soft-kill (to let node clean up the browser processes)
            print("TIMEOUT", flush=True)
            try:
                proc.wait(timeout=time_to_kill_limit)
            except subprocess.TimeoutExpired:
                proc.kill()
                print("HARD-KILLED", flush=True)
        except:
            proc.kill() # hard-kill since this is something bad/fatal
            raise
    # remember: implicit proc.wait() on __exit__ from with statement
    return status


def main(argv):
    if len(sys.argv) == 1:
        print(f"usage: {argv[0]} [URL1 [URL2 [...]]]")
        exit(2)

    for url in sys.argv[1:]:
        hostname = urlparse(url).hostname
        munged_url = REPLACEX.sub("_", url)[:64]
        random_tag = hashlib.md5(url.encode('utf8')).hexdigest()
        collection_dir = os.path.join(TAG, hostname, f"{munged_url}.{random_tag}")

        os.makedirs(collection_dir, exist_ok=False)
        log_filename = os.path.join(collection_dir, "crawl.log")
        print(f"Crawling '{url}' (dir={collection_dir})...", flush=True)

        cmd_argv = [
            "npm",
            "run",
            "crawl",
            "--",
            "-b",
            BROWSER_EXE,
            "-o",
            os.path.abspath(collection_dir),
            "-t",
            str(TIME_LIMIT),
            "-u",
            url,
            "--debug=debug",
        ]

        if PROFILE == False:
            cmd_argv += [
                "-s", "down"
            ]
        else:
            cmd_argv += [
                "-e", PROFILE
            ]

        with open(log_filename, "wt", encoding="utf-8") as log:
            cmd_options = {
                "cwd": NPM_CWD,
                "stdout": log,
                "stderr": subprocess.STDOUT,
                "TIME_OUT": TIME_OUT,
                "TIME_TO_KILL": TIME_TO_KILL,
            }
            run_with_timeout(cmd_argv, **cmd_options)


if __name__ == "__main__":
    main(sys.argv)
