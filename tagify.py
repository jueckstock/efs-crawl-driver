#!/usr/bin/env python3
import csv
import hashlib
import os
import re
import sys
from urllib.parse import urlparse

from loguru import logger

REPLACEX = re.compile(r"[^-_a-zA-Z0-9]")
BOOMEX = re.compile(r"^ERROR |FATAL:", re.MULTILINE)


def main(argv):
    try:
        root_dir = argv[1]
    except IndexError:
        print(f"usage: {argv[0]} ROOT_DIR <URL_LIST")
        return
    
    profiles = [d for d in os.listdir(root_dir) if os.path.isdir(os.path.join(root_dir, d))]
    profiles.sort()

    wtr = csv.writer(sys.stdout)
    wtr.writerow(['order', 'site_tag', 'crawl_url'] + profiles)
    for order, line in enumerate(sys.stdin):
        url = line.strip()
        hostname = urlparse(url).hostname
        munged_url = REPLACEX.sub("_", url)[:64]
        random_tag = hashlib.md5(url.encode('utf8')).hexdigest()
        site_tag = os.path.join(hostname, f"{munged_url}.{random_tag}")

        pfails = [None for _ in profiles]
        for i, p in enumerate(profiles):
            try:
                with open(os.path.join(root_dir, p, site_tag, "crawl.log"), "r", encoding="utf8") as fd:
                    blob = fd.read()
                m = BOOMEX.search(blob)
                pfails[i] = bool(m)
            except:
                logger.exception("whoopsie")
        wtr.writerow([order, site_tag, url] + pfails)


if __name__ == "__main__":
    main(sys.argv)