#!/usr/bin/env python3
import csv
import multiprocessing
import itertools
import os
import re
import sys
from collections import namedtuple
from http import cookies
from typing import Iterable, Sequence, Set, Tuple
from urllib.parse import parse_qsl, urlparse

from loguru import logger
import networkx as nx
import numpy as np
import pandas as pd
from publicsuffix2 import get_sld

from common import graphs_in_dir, walk_experiment_trees

HalfTokenTuple = namedtuple('HalfTokenTuple', ['http_etld1', 'key', 'value'])

TokenTuple = namedtuple('TokenTuple', ['tag', 'site_etld1', 'http_etld1', 'key', 'value'])

RE_HEADER_LINE = re.compile(r'^([-a-zA-Z0-9_]+):"(\S+)" "(.*)"$')


def raw_header_pairs(encoded_headers: str, prefix_filter: str = None) -> Iterable[Tuple[str, str, str]]:
    for line in encoded_headers.split('\n'):
        m = RE_HEADER_LINE.match(line)
        if m:
            if (prefix_filter is None) or (prefix_filter == m.group(1)):
                yield m.group(1), m.group(2), m.group(3)
        elif line:
            raise ValueError(f"malformed encoded header: '{line}'")


def half_token_tuples_in_dir(directory: str) -> Iterable[HalfTokenTuple]:
    for graph in graphs_in_dir(directory):
        resource_nodes = [k for k, v in nx.get_node_attributes(graph, "node type").items() if v == "resource"]
        for k, v in nx.get_node_attributes(graph, "node type").items():
            if v == 'resource':
                try:
                    url_fields = urlparse(graph.nodes[k]["url"])
                    http_etld1 = get_sld(url_fields.hostname)

                    # key/value tokens from query string params (unofficial standard format)
                    for key, value in parse_qsl(url_fields.query):
                        yield HalfTokenTuple(http_etld1, "query:" + key, value)
                    
                    # key/value tokens from request headers (out-edges only)
                    for n1, n2, eid, raw_headers in graph.out_edges(k, data="value", keys=True, default=""):
                        for _, key, value in raw_header_pairs(raw_headers, prefix_filter="raw-request"):
                            if key.lower() == "cookie":
                                for morsel in cookies.SimpleCookie(value).values():
                                    yield HalfTokenTuple(http_etld1, "cookie:" + morsel.key, morsel.value)
                            else:
                                yield HalfTokenTuple(http_etld1, "header:" + key, value)
                except:
                    logger.exception("error handling HTTP resource:")


def list_full_tokens_in_dir(directory: str) -> Set[TokenTuple]:
    # hacks to extract tag/site-eTLD+1 from directory structure [tag is pretty much like that; site eTLD+1 could come from DOM root URLs if we have unified graphs]
    tag = os.path.basename(os.path.dirname(os.path.dirname(directory)))
    site_etld1 = os.path.basename(os.path.dirname(directory))

    return {TokenTuple(tag, site_etld1, h, k, v) for h, k, v in half_token_tuples_in_dir(directory)}


def main(argv):
    if len(argv) < 2:
        print(f"usage: {argv[0]} DIR1 [DIR2 [DIR3 [...]]]")
        return
    
    directories = argv[1:]
    tags = [os.path.basename(d) for d in directories]

    writer = csv.writer(sys.stdout, dialect='excel', lineterminator='\n')
    writer.writerow(['tag', 'site_etld1', 'http_etld1', 'key', 'value'])
    with multiprocessing.Pool(processes=len(directories)) as pool:
        for _, *dirs in walk_experiment_trees(directories):
            token_sets = pool.map(list_full_tokens_in_dir, dirs, chunksize=1)
            writer.writerows(itertools.chain(*token_sets))


if __name__ == "__main__":
    main(sys.argv)
