#!/usr/bin/env python3
import csv
import itertools
import multiprocessing
import os
import re
import sys
from collections import namedtuple
from http import cookies
from typing import Iterable, Optional, Sequence, Set, Tuple
from urllib.parse import parse_qsl, urlparse

import networkx as nx
import numpy as np
import pandas as pd
from loguru import logger
from publicsuffix2 import get_sld

SITE_THRESHOLD = int(os.environ.get("SITE_THRESHOLD", 1))
LENGTH_FLOOR = int(os.environ.get("LENGTH_FLOOR", 1))
ROOT = bool(os.environ.get("ROOT", False))


def main(argv):
    if len(argv) < 3:
        print(f"usage: {argv[0]} PRIVACY_METRICS_CSV (lat|long) [src1 [src2 [...]]]")
        return
    
    privacy_csv = argv[1]
    csv_stem = os.path.splitext(privacy_csv)[0]

    mode = argv[2].lower()
    assert mode in ('lat', 'long'), "Invalid mode!"

    df = pd.read_csv(privacy_csv)
    if 'is_root' in df.columns:
        if ROOT:
            df = df[df.is_root == True].drop(['is_root', 'is_ad'], axis=1)
            csv_stem += "_root"
            subset = "Root/1p Frames"
        else:
            df = df[(df.is_root == False) & (df.is_ad == False)].drop(['is_root', 'is_ad'], axis=1)
            csv_stem += "_3pnoad"
            subset = "Non-Ad 3p Frames"
    else:
        subset = "All Frames"
    
    source_tag = "_" + '-'.join(a.lower() for a in argv[3:]) if argv[3:] else ''
    privacy_pdf = f"{csv_stem}_{mode}{source_tag}.pdf"
    
    sources = argv[3:]
    if sources:
        df = df[df.source.isin(sources)]
    df = df.drop('source', axis=1)

    counts = df.groupby(['http_etld1', 'key', 'value']).nunique()
    if mode == 'lat':
        distinctive_flows = counts[(counts.profile == 1) & (counts.site_etld1 > SITE_THRESHOLD)]
        xfield = 'http_etld1'
        yfield = 'site_etld1'
    else:
        distinctive_flows = counts[(counts.profile == 1) & (counts.session > 1)]
        xfield = 'site_etld1'
        yfield = 'http_etld1'
    #distinctive_flows.to_csv("scratch-flows.csv")
    
    privacy_tokens = df.set_index(['http_etld1', 'key', 'value']).loc[distinctive_flows.index]
    #privacy_tokens.to_csv("scratch-rows.csv")
    
    trackability = privacy_tokens.reset_index().groupby([xfield, 'profile'])[yfield].nunique().unstack(fill_value=0)
    #trackability.to_csv("scratch-tracking.csv")


    wut = trackability.cumsum()
    #print(wut)
    ax = wut.plot()
    fig = ax.get_figure()
    fig.savefig(privacy_pdf)


if __name__ == "__main__":
    main(sys.argv)
