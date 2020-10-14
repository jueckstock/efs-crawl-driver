#!/usr/bin/env python
import sys
import re
import os

import pandas as pd
from matplotlib import pyplot as plt

STYLE=os.environ.get("STYLE", "tableau-colorblind10")


def main(argv):
    try:
        error_csv = argv[1]
    except IndexError:
        print(f"usage: {argv[0]} ERROR_MATRIX_CSV")
    
    df = pd.read_csv(error_csv)
    df.policy = df.policy.str.replace(re.compile(r"^(\w+)$"), lambda m: {
        'vanilla': 'Permissive',
        'prototype': 'Page-length',
        'splitkey': 'Site-keyed',
        'fullblock3p': 'Blocking',
    }.get(m.group(1), m.group(1)))
    wut = df.loc[df.policy != 'halfblock3p'].groupby('policy').sum()
    
    total = wut['total_logs'].mode()[0]
    wut['total_errors'] = wut[['non_fatal', 'fatal_non_pg', 'fatal_pg']].transpose().sum().transpose()
    wut['total_ok'] = total - wut['total_errors']
    wat = wut[['total_ok', 'non_fatal', 'fatal_non_pg', 'fatal_pg']] / total

    wat.rename(columns={
        'total_ok': 'successful',
        'non_fatal': 'non-crash error',
        'fatal_non_pg': 'non-PG crash',
        'fatal_pg': 'PG crash',
    }, inplace=True)
    wat = wat.reindex(index=[
        'Blocking',
        'Site-keyed',
        'Page-length',
        'Permissive',
    ])
    
    plt.style.use(STYLE)
    ax = wat.plot.barh(stacked=True, ylim=(0, .20))
    MAXP = 100
    ax.set_xticks([round(i / 100, 2) for i in range(0, MAXP+1, 10)], minor=False)
    ax.set_xticklabels([format(round(i / 100, 2), ".0%") for i in range(0, MAXP+1, 10)], minor=False)
    ax.set_xticks([round(i / 100, 2) for i in range(0, MAXP+1, 5)], minor=True)
    ax.grid(axis="x", which="both")
    ax.set_axisbelow(True)
    ax.set_ylabel(None)
    
    
    fig = ax.get_figure()
    fig.tight_layout()
    fig.savefig("errors.pdf")
    plt.show()


if __name__ == "__main__":
    main(sys.argv)