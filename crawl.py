#!/usr/bin/env python3
import os
import sys
import subprocess
import hashlib
import random
import re
import string
from urllib.parse import urlparse

REPLACEX = re.compile(r"[^-_a-zA-Z0-9]")

TAG = os.environ.get("TAG", 'tag')
PROFILE = os.environ.get("PROFILE", False)
TIME_LIMIT = float(os.environ.get("TIME_LIMIT", 15.0))
TIME_OUT = float(os.environ.get("TIME_OUT", max(TIME_LIMIT, 1.0) * 4))

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
            "/home/jjuecks/brave/Static/brave",
            "-o",
            os.path.abspath(collection_dir),
            "-t",
            str(TIME_LIMIT),
            "-u",
            url,
            "--debug=verbose",
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
                "cwd": "/home/jjuecks/brave/pagegraph-crawl",
                "stdout": log,
                "stderr": subprocess.STDOUT,
            }
            with subprocess.Popen(cmd_argv, **cmd_options) as proc:
                try:
                    status = proc.wait(timeout=TIME_OUT)
                    if status != 0:
                        print(f"WARNING: status={status}", flush=True)
                except subprocess.TimeoutExpired:
                    proc.terminate() # soft-kill (to let node clean up the browser processes)
                    print("TIMEOUT", flush=True)
                except:
                    proc.kill() # hard-kill since this is something bad/fatal
                    raise


if __name__ == "__main__":
    main(sys.argv)
