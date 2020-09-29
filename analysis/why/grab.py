#!/usr/bin/env python3
import csv
import glob
import os
import re
import shutil
import sys
from collections import namedtuple
from urllib.parse import urlparse

from loguru import logger
from publicsuffix2 import get_sld

RE_EXTRACT_META_TAGS = re.compile(r"<url>(.*?)</url>\s*<is_root>(true|false)</is_root>")

MetaTags = namedtuple('MetaTags', ['filename', 'url', 'etld1', 'is_root'])


def get_meta_tags(filename: str) -> MetaTags:
    with open(filename, "rt", encoding="utf8") as fd:
        blob = fd.read()
        m = RE_EXTRACT_META_TAGS.search(blob)
        if not m:
            raise ValueError("no tags found")
        url = m.group(1)
        ubits = urlparse(url)
        etld1 = get_sld(ubits.hostname)
        is_root = m.group(2) == "true"
        return MetaTags(filename, url, etld1, is_root)


def main(argv):
    try:
        root_dir = argv[1]
    except IndexError:
        print(f"usage: {argv[0]} ROOT_DIR [SITE_TAGS...]")
        return
    profiles = [d for d in os.listdir(root_dir) if os.path.isdir(os.path.join(root_dir, d))]

    for site_tag in argv[2:]:
        try:
            os.makedirs(site_tag)
            with open(os.path.join(site_tag, "graph_summary.csv"), "wt", encoding="utf8") as fout:
                wc = csv.writer(fout)
                wc.writerow(['profile', 'basename', 'is_root', 'url_etld1', 'url_full'])
                for p in profiles:
                    cdir = os.path.join(root_dir, p, site_tag)
                    shutil.copytree(cdir, os.path.join(site_tag, p))
                    graph_files = glob.glob(os.path.join(cdir, "*.graphml"))
                    for mt in map(get_meta_tags, graph_files):
                        wc.writerow([p, os.path.basename(mt.filename), mt.is_root, mt.etld1, mt.url])
        except Exception as ex:
            logger.exception(f"failed to process '{site_tag}'")


if __name__ == "__main__":
    main(sys.argv)

