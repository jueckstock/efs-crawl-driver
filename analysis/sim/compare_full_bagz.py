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

FIXED_EDGE_SET = os.environ.get("FIXED_EDGE_SET", None)
SET_CLASS = Multiset if os.environ.get("MULTISET", False) else set

RE_NODE_PATTERN = re.compile("^(\w+)(?:\[|$)")
RE_EDGE_PATTERN = re.compile("^(\w+):")

ALL_NODE_TYPES = [
    "RemoteFrame",
    "Resource",
    "WebAPI",
    "JsBuiltin",
    "Html",
    "Text",
    "DomRoot",
    "FrameOwner",
    "LocalStorage",
    "SessionStorage",
    "CookieJar",
    "Script",
    "Parser",
]

ALL_EDGE_TYPES = [
    "TextChange",
    "CreateNode",
    "InsertNode",
    "RemoveNode",
    "DeleteNode",
    "JsCall",
    "Execute",
    "RequestStart",
    "RequestError",
    "RequestComplete",
    "AddEventListener",
    "RemoveEventListener",
    "EventListener",
    "StorageSet",
    "ReadStorageCall",
    "DeleteStorage",
    "ClearStorage",
    "ExecuteFromAttribute",
    "SetAttribute",
    "DeleteAttribute",
]


def jaccard_index(a: SET_CLASS, b: SET_CLASS) -> float:
    num = len(a & b)
    den = len(a | b)
    return num / den if den > 0 else np.nan


def ok_match(match, allowed_things: list) -> bool:
    return match and (match.group(1) in allowed_things)


def load_set(
    file_name: str,
    allowed_nodes: list = ALL_NODE_TYPES,
    allowed_edges: list = ALL_EDGE_TYPES,
) -> SET_CLASS:
    s = SET_CLASS()
    regex = None
    with open(file_name, "rt", encoding="utf8") as fd:
        line = next(fd).strip()
        m = RE_EDGE_PATTERN.match(line)
        if m:
            regex = RE_EDGE_PATTERN
            allowed_things = allowed_edges
        else:
            m = RE_NODE_PATTERN.match(line)
            if m:
                regex = RE_NODE_PATTERN
                allowed_things = allowed_nodes
            else:
                raise ValueError(f"unknown set member type ({line})")

        if ok_match(m, allowed_things):
            s.add(line)

        for line in fd:
            line = line.strip()
            m = regex.match(line)
            if not m:
                raise ValueError(f"unexpected set member type ({line})")
            # print(m.groups(), file=sys.stderr)
            if ok_match(m, allowed_things):
                s.add(line)

    return s


def process_directories(url_files: list, allowed_node_types: list = ALL_NODE_TYPES, allowed_edge_types: list = ALL_EDGE_TYPES):
    for url_file in url_files:
        # print(url_file, file=sys.stderr)
        *_, hostname, crawl_url_tag, _, _ = url_file.split(os.sep)
        site_tag = os.path.join(hostname, crawl_url_tag)
        with open(url_file, "rt", encoding="utf8") as fd:
            frame_url = fd.read().strip()

        work_dir = os.path.dirname(url_file)
        
        #nbags = {}
        #for bf in glob.glob(os.path.join(work_dir, "*.nbag")):
        #    p = os.path.splitext(os.path.basename(bf))[0]
        #    nbags[p] = load_set(bf, allowed_node_types)

        ebags = {}
        for bf in glob.glob(os.path.join(work_dir, "*.ebag")):
            p = os.path.splitext(os.path.basename(bf))[0]
            ebags[p] = load_set(bf, allowed_edge_types)

        profiles = list(sorted(ebags.keys()))
        #for p1, p2 in itertools.combinations(profiles, 2):
        p2 = profiles[-1]
        for p1 in profiles[:-1]:
            #nji = jaccard_index(nbags[p1], nbags[p2])
            eji = jaccard_index(ebags[p1], ebags[p2])
            #yield (site_tag, frame_url, p1, p2, nji, eji)
            yield (site_tag, frame_url, p1, p2, eji)


def main(argv):
    url_files = argv[1:]
    wtr = csv.writer(sys.stdout, lineterminator="\n")
    
    wtr.writerow([
            "edge_mask",
            "site_tag",
            "frame_url",
            "p1",
            "p2",
            #"node_jaccard",
            "edge_jaccard",
        ])
    for allowed_bits in itertools.product(range(2), repeat=len(ALL_EDGE_TYPES)):
        allowed_edge_mask = "".join(map(str, allowed_bits))
        allowed_edge_types = [
            ALL_EDGE_TYPES[i] for i, b in enumerate(allowed_bits) if b
        ]
        for stats_row in process_directories(url_files, ALL_NODE_TYPES, allowed_edge_types):
            wtr.writerow((allowed_edge_mask,) + stats_row)


if __name__ == "__main__":
    main(sys.argv)