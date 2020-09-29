"""common: utilties for extracting stats from parallel crawls
"""
import glob
import itertools
import json
import multiprocessing
import os
import re
import subprocess
from collections import defaultdict, namedtuple
from typing import (Any, Callable, Iterable, Mapping, Optional, Sequence,
                    Tuple, Union)
from xml.etree import ElementTree

import multiset
import networkx as nx
import numpy as np
import pandas as pd
from loguru import logger

PageGraphMetadata = namedtuple('PageGraphMetadata', ['version', 'url', 'is_root', 'timespan'])


def get_graphml_meta(graphml_file: str) -> PageGraphMetadata:
    ns = {'g': 'http://graphml.graphdrawing.org/xmlns'}
    etree = ElementTree.parse(graphml_file)
    root = etree.getroot()
    desc = root.find('g:desc', ns)
    is_root_text = desc.find('g:is_root', ns).text
    time_start_text = desc.find('g:time/g:start', ns).text
    time_end_text = desc.find('g:time/g:end', ns).text
    return PageGraphMetadata(
        desc.find('g:version', ns).text,
        desc.find('g:url', ns).text, 
        is_root_text == "true",
        (float(time_start_text), float(time_end_text)))


def walk_experiment_trees(root_map: Mapping[str, str]) -> Iterable[Sequence[str]]:
    graphml_dirs = defaultdict(dict)
    for tag, root in root_map.items():
        for node, _, files in os.walk(root):
            if any(f.endswith(".graphml") for f in files):
                stem = os.path.relpath(node, root)
                pre_stem = node[:-len(stem)]
                graphml_dirs[stem][tag] = node
    
    tags = list(root_map.keys())
    for stem, dir_map in graphml_dirs.items():
        yield [stem] + [dir_map.get(t) for t in tags]


def graphs_in_dir(directory: Optional[str], with_filename: bool = False) -> Iterable[Union[nx.MultiDiGraph, Tuple[nx.MultiDiGraph, str]]]:
    if directory is not None:
        for fn in glob.glob(os.path.join(directory, "*.graphml")):
            if with_filename:
                yield nx.read_graphml(fn), fn
            else:
                yield nx.read_graphml(fn)


ABRC_EXE = os.environ.get("ABRC_EXE", os.path.join(os.path.dirname(__file__), "..", "abrc", "target", "release", "abrc"))
ABRC_FSF = os.environ.get("ABRC_FSF", "filterset.dat")

RE_FRAME_ID = re.compile(r"^page_graph_([0-9A-Fa-f]{32})\.(\d+)\.graphml$")

FrameRoot = namedtuple('FrameRoot', ['graph', 'frame_url', 'site_url'])


def filter_frame_loaders(frame_roots: Sequence[FrameRoot]) -> Sequence[bool]:
    if not frame_roots:
        return []
    
    json_input = "\n".join(json.dumps({"url": f.frame_url, "source_url": f.site_url, "request_type": "sub_frame"}) for f in frame_roots)
    proc = subprocess.run([ABRC_EXE, "filter", "-f", ABRC_FSF], input=json_input, capture_output=True, check=True, encoding="utf8")
    return list(map(json.loads, proc.stdout.split()))


RE_URL_TAG = re.compile(rb"<url>([^<>]+)</url>")


def find_3p_nonad_graphs(directory: Optional[str]) -> list:
    """this is kind of hacky/broken right now, but so is our data and I'm tired of dealing with it"""
    from xml.sax.saxutils import unescape
    sub_frames = []
    if directory:
        origin_host = os.path.basename(os.path.dirname(directory))
        origin_url = f"https://{origin_host}/"
        
        for graph, filename in graphs_in_dir(directory, with_filename=True):
            with open(filename, "rb") as fd:
                blob = fd.read()
                is_root = b"<is_root>true</is_root>" in blob
                raw_url = RE_URL_TAG.search(blob).group(1)
                root_url = unescape(raw_url.decode('utf8'))
            #meta = get_graphml_meta(filename)
            if not is_root: #meta.is_root:
                sub_frames.append(FrameRoot(graph, root_url, origin_url)) #meta.url, origin_url))
    
    frame_ad_matches = filter_frame_loaders(sub_frames)
    return [sf.graph for sf, ad in zip(sub_frames, frame_ad_matches) if not ad]

    """ gmap = defaultdict(dict)
    fmap = defaultdict(dict)
    for graph, filename in graphs_in_dir(directory, with_filename=True):
        meta = get_graphml_meta(filename)
        m = RE_FRAME_ID.match(os.path.basename(filename))
        frame_id = m.group(1)
        frame_version = m.group(2)
        gmap[frame_id][frame_version] = (meta, graph)

        if meta.is_root:
            fmap[frame_id][frame_version] = meta.url
        
        for n, nt in graph.nodes(data="node type"):
            if nt == "remote frame":
                rfid = graph.nodes[n]["frame id"]
                fmap[rfid][-1] = meta.url
    
    sub_frames = []
    for frame_id, versions in gmap.items():
        for frame_version, (frame_meta, frame_graph) in versions.items():
            if not frame_meta.is_root:
                site_url = fmap[frame_id].get(-1)
                if not site_url:
                    logger.warning(f"mystery frame '{frame_id}' has no known site_url? (in {directory})")
                else:
                    frame_url = frame_meta.url
                    sub_frames.append(FrameRoot(frame_graph, frame_url, site_url))
    
    frame_ad_matches = filter_frame_loaders(sub_frames)
    return [sf.graph for sf, ad in zip(sub_frames, frame_ad_matches) if not ad] """


def jaccard_index(a: multiset.Multiset, b: multiset.Multiset) -> float:
    num = len(a.intersection(b))
    den = len(a.union(b))
    return num / den if den else np.nan


def parallel_stats(root_map: Mapping[str, str], extractor: Callable[[Optional[str]], multiset.Multiset]) -> Iterable[Tuple[str, Sequence[Any]]]:
    with multiprocessing.Pool(processes=len(root_map)) as pool:
        for stem, *dirs in walk_experiment_trees(root_map):
            things = pool.map(extractor, dirs, chunksize=1)
            yield (stem, things)


def parallel_ji_distros(root_map: Mapping[str, str], bagger: Callable[[Optional[str]], multiset.Multiset]) -> pd.DataFrame:
    scores = defaultdict(list)
    tags = list(root_map)
    for _, bags in parallel_stats(root_map, bagger):
        for (t1, bag1), (t2, bag2) in itertools.combinations(zip(tags, bags), 2):
            ji = jaccard_index(bag1, bag2)
            scores[f"{t1}/{t2}"].append(ji)

    return pd.DataFrame(scores)


_RE_PROFILE_FIELDS = re.compile(r"^(\w+)\d+$")

def get_profile_groups(tags: Iterable[str]) -> Mapping[str, Sequence[str]]:
    groups = defaultdict(list)
    for tag in tags:
        m = _RE_PROFILE_FIELDS.match(tag)
        if m:
            groups[m.group(1)].append(tag)
        else:
            raise ValueError(tag)
    return groups


def rank_distinguished_items(
    raw_df: pd.DataFrame,
    group_fields: Sequence[str],
    value_field: str,
    profile_groups: Mapping[str, Sequence[str]],
) -> pd.DataFrame:
    ptt = raw_df.groupby(group_fields)[value_field].sum().unstack(fill_value=0).transpose()
    global_variance = ptt.var()
    series = {
        'GLOBAL': global_variance,
        'SUM': np.zeros_like(global_variance),
    }
    for profile, instances in profile_groups.items():
        profile_variance = ptt.loc[instances].var()
        series[profile] = profile_variance
        series['SUM'] += profile_variance
    vtt = pd.DataFrame(series)
    return (vtt.GLOBAL - vtt.SUM).sort_values(ascending=False)
