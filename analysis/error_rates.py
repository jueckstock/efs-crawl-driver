#!/usr/bin/env python
import sys

import pandas as pd


def main(argv):
    try:
        error_csv = argv[1]
    except IndexError:
        print(f"usage: {argv[0]} ERROR_MATRIX_CSV")
    
    df = pd.read_csv(error_csv)
    wut = df.loc[df.policy != 'halfblock3p'].groupby('policy').sum()
    
    total = wut['total_logs'].mode()[0]
    wat = wut[['non_fatal', 'fatal_non_pg', 'fatal_pg']] / total
    ax = wat.plot.bar(stacked=True, ylim=(0, .20))
    fig = ax.get_figure()
    fig.savefig("errors.pdf")





if __name__ == "__main__":
    main(sys.argv)