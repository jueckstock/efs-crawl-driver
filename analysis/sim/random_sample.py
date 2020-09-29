#!/usr/bin/env python3
import csv
import glob
import os
import re
import sys
from xml.sax.saxutils import unescape

import pandas as pd

RE_EXTRACT_META_TAGS = re.compile(r"<url>(.*?)</url>\s*<is_root>(true|false)</is_root>")


def main(argv):
    try:
        bagz_file = argv[1]
        urls_file = argv[2]
        root_dir = argv[3]
    except IndexError:
        print(f"usage: {argv[0]} BAGZ_FILE URLS_FILE ROOT_DIR")
        return
    
    bdf = pd.read_csv(bagz_file)
    udf = pd.read_csv(urls_file)

    PROFILES = list(set(bdf.p1.unique()) | set(bdf.p2.unique()))

    err_df = udf.set_index('site_tag').drop(['order', 'crawl_url'], axis=1).transpose().any().transpose()
    err_df = err_df[err_df == False]
    bdf = bdf[bdf.site_tag.isin(err_df.index)]

    selected = bdf.groupby(['site_tag', 'frame_url', 'p2', 'p1']).node_jaccard.sum().unstack().dropna().sample(n=100)

    wtr = csv.writer(sys.stdout, lineterminator="\n")
    wtr.writerow(['status', 'site_tag', 'frame_url', 'profile', 'graphml_file'])
    for (site_tag, frame_url) in selected.reset_index()[['site_tag', 'frame_url']].values:
        for p in PROFILES:
            tdir = os.path.join(root_dir, p, site_tag)
            matching_graph_files = []
            for gf in glob.glob(os.path.join(tdir, "*.graphml")):
                with open(gf, "rt", encoding="utf8") as fd:
                    contents = fd.read()
                m = RE_EXTRACT_META_TAGS.search(contents)
                if m and unescape(m.group(1)) == frame_url:
                    matching_graph_files.append(gf)
            if len(matching_graph_files) == 1:
                status = 'ok'
            elif len(matching_graph_files) > 1:
                status = 'dup'
            else:
                status = 'n/a'
            wtr.writerow([status, site_tag, frame_url, p, matching_graph_files[-1] if matching_graph_files else None])


if __name__ == "__main__":
    main(sys.argv)