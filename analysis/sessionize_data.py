#!/usr/bin/env python3
import functools
import os
import sys

import pandas as pd


def sessionize(name_tag_pair: str, field: str = "session") -> pd.DataFrame:
    filename, tag = name_tag_pair.split(':')
    df = pd.read_csv(filename)
    df[field] = tag
    return df


def main(argv):
    try:
        *sources, dest = argv[1:]
        if len(sources) < 1:
            raise ValueError("not enough sources")
    except ValueError:
        print(f"usage: {argv[0]} CSV_FILE1:TAG1 [CSV_FILE2:TAG2 [...]] DEST_CSV_FILE")
        return
    
    first_source, *later_sources = sources
    df = functools.reduce(lambda a, b: a.append(sessionize(b)), later_sources, sessionize(first_source))
    df.to_csv(dest, index=False)


if __name__ == "__main__":
    main(sys.argv)