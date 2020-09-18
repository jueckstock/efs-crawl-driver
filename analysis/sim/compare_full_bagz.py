#!/usr/bin/env python3
import csv
import glob
import itertools
import os
import pickle
import re
import sys
from collections import defaultdict

import numpy as np
from multiset import Multiset

SET_CLASS = Multiset if os.environ.get("MULTISET", False) else set

RE_NODE_PATTERN = re.compile("^(\w+)(?:\[|$)")
RE_EDGE_PATTERN = re.compile("^(\w+)\[?.*?->(\w+)(?:\[|$)")

ALL_NODE_TYPES = ['DomRoot', 'LocalStorage', 'SessionStorage', 'Resource', 'WebAPI', 'Script', 'JsBuiltin', 'CookieJar', 'Html', 'FrameOwner', 'Text']


def jaccard_index(a: SET_CLASS, b: SET_CLASS) -> float:
    num = len(a & b)
    den = len(a | b)
    return num / den if den > 0 else np.nan


def ok_node(match, allowed_nodes: list = ALL_NODE_TYPES) -> bool:
    kind = match.group(1)
    return kind in allowed_nodes


def ok_edge(match, allowed_nodes: list = ALL_NODE_TYPES) -> bool:
    kind1 = match.group(1)
    kind2 = match.group(2)
    return (kind1 in allowed_nodes) and (kind2 in allowed_nodes)


def load_set(file_name: str, allowed_nodes: list = ALL_NODE_TYPES) -> SET_CLASS:
    s = SET_CLASS()
    regex = None
    ok_func = None
    with open(file_name, "rt", encoding="utf8") as fd:
        line = next(fd).strip()
        m = RE_EDGE_PATTERN.match(line)
        if m:
            regex = RE_EDGE_PATTERN
            ok_func = ok_edge
        else:
            m = RE_NODE_PATTERN.match(line)
            if m:
                regex = RE_NODE_PATTERN
                ok_func = ok_node
            else:
                raise ValueError(f"unknown set member type ({line})")
        
        if ok_func(m, allowed_nodes):
            s.add(line)

        for line in fd:
            line = line.strip()
            m = regex.match(line)
            if not m:
                raise ValueError(f"unexpected set member type ({line})")
            #print(m.groups(), file=sys.stderr)
            if ok_func(m):
                s.add(line)
    
    return s


def main(argv):
    wtr = csv.writer(sys.stdout, lineterminator="\n")
    wtr.writerow(['node_mask', 'site_tag', 'frame_url', 'p1', 'p2', 'node_jaccard', 'edge_jaccard'])
    for allowed_bits in itertools.product(range(2), repeat=len(ALL_NODE_TYPES)):
        allowed_node_mask = ''.join(map(str, allowed_bits))
        allowed_node_types = [ALL_NODE_TYPES[i] for i, b in enumerate(allowed_bits) if b]

        for url_file in argv[1:]:
            #print(url_file, file=sys.stderr)
            *_, hostname, crawl_url_tag, _, _ = url_file.split(os.sep)
            site_tag = os.path.join(hostname, crawl_url_tag)
            with open(url_file, "rt", encoding="utf8") as fd:
                frame_url = fd.read().strip()
            
            work_dir = os.path.dirname(url_file)
            nbags = {}
            for bf in glob.glob(os.path.join(work_dir, "*.nbag")):
                p = os.path.splitext(os.path.basename(bf))[0]
                nbags[p] = load_set(bf, allowed_node_types)
            
            ebags = {}
            for bf in glob.glob(os.path.join(work_dir, "*.ebag")):
                p = os.path.splitext(os.path.basename(bf))[0]
                ebags[p] = load_set(bf, allowed_node_types)

            profiles = list(sorted(nbags.keys()))
            for p1, p2 in itertools.combinations(profiles, 2):
                nji = jaccard_index(nbags[p1], nbags[p2])
                eji = jaccard_index(ebags[p1], ebags[p2])
                wtr.writerow([allowed_node_mask, site_tag, frame_url, p1, p2, nji, eji])


if __name__ == "__main__":
    main(sys.argv)