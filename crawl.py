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
TIME_OUT = max(TIME_LIMIT, 1.0) * 2.0

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
            "/home/jjuecks/brave/brave-browser/src/out/Static/brave",
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
                "check": False,
                "timeout": TIME_OUT,
            }
            try:
                subprocess.run(cmd_argv, **cmd_options)
            except subprocess.TimeoutExpired:
                print("TIMEOUT", flush=True)


if __name__ == "__main__":
    main(sys.argv)
