#!/usr/bin/env python3
import os
import sys
from collections import defaultdict
from functools import reduce

import pandas as pd
from matplotlib import pyplot as plt
from publicsuffix2 import get_sld


def graph_counts_by_profile(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby(['site_tag', 'profile_tag']).url.count().unstack(fill_value=0)


def pred_all_same(df: pd.DataFrame) -> pd.DataFrame:
    return df.transpose().apply(lambda x: all(x[0] == y for y in x[1:])).transpose()


def main(argv):
    # per-graph data
    csv_filename = argv[1]
    csv_stem = os.path.splitext(csv_filename)[0]
    orig_df = pd.read_csv(csv_filename)

    # optional per-site-tag error data used to filter out rows from URLs that encountered errors
    if len(argv) > 2:
        url_df = pd.read_csv(argv[2])
        err_df = url_df.set_index('site_tag').drop(['order', 'crawl_url'], axis=1).transpose().any().transpose()
        err_df = err_df[err_df == False]
        orig_df = orig_df[orig_df.site_tag.isin(err_df.index)]
    else:
        url_df = None
    
    # augment with eTLD+1 extracted from site-tag (i.e., from the crawl URL hostname)
    orig_df['site_etld1'] = orig_df['site_tag'].apply(lambda x: get_sld(x.split('/')[0]))

    # all graphs
    adf = orig_df.drop(['is_root', 'is_ad'], axis=1)

    # 1p-only graphs
    rdf = orig_df[orig_df.is_root == True].drop(['is_root', 'is_ad'], axis=1)

    # 3p-no-ad graphs
    tdf = orig_df[(orig_df.is_root == False) & (orig_df.is_ad == False)].drop(['is_root', 'is_ad'], axis=1)

    # 3p-ad graphs (just for fun)
    zdf = orig_df[(orig_df.is_root == False) & (orig_df.is_ad == True)].drop(['is_root', 'is_ad'], axis=1)


    # TEST: can we identify "first-use" subsets for each (profile/frame-etld1) tuple?
    #--------------------------------------------------------------------------------
    assert url_df is not None, "need URL list for order information..."
    FIELDS = """profile_tag,url_etld1,total_nodes,total_edges,total_dom_nodes,total_remote_frames,touched_dom_nodes,completed_requests,event_listenings,post_storage_script_edges,post_storage_console_errors""".split(",")


    for (dataset_label, wutdf) in [("3p-no-ad", tdf), ("3p-ad-only", zdf)]:
        mongo_df = wutdf.join(url_df.set_index("site_tag"), on="site_tag").sort_values('order')

        # sub-set test: (global-first-median, site-first-median, overall-median) comparisons? [bust]
        """ global_firsts = [] #defaultdict(list)
        site_firsts = [] #defaultdict(list)
        for (url_etld1, profile_tag), records in mongo_df.groupby(['url_etld1', 'profile_tag']):
            ordered_records = records.sort_values('order')
            global_firsts.append(ordered_records.iloc[0][FIELDS])
            for site_etld1, site_records in ordered_records.groupby("site_etld1"):
                site_firsts.append(site_records.sort_values('order').iloc[0][FIELDS])
        gfdf = pd.DataFrame(global_firsts)
        sfdf = pd.DataFrame(site_firsts)
        print(gfdf.groupby('profile_tag').median())
        print(sfdf.groupby('profile_tag').median())
        print(mongo_df[FIELDS].groupby('profile_tag').median()) """

        global_firsts = []
        site_firsts = []
        for (url_etld1, profile_tag), records in mongo_df.groupby(['url_etld1', 'profile_tag']):
            ordered_records = records.sort_values('order')
            global_firsts.append(ordered_records.iloc[0][FIELDS])
            for site_etld1, site_records in ordered_records.groupby("site_etld1"):
                site_firsts.append(site_records.sort_values('order').iloc[0][FIELDS])
        gfdf = pd.DataFrame(global_firsts)
        sfdf = pd.DataFrame(site_firsts)

        for field in FIELDS[2:]:
            fig, axen = plt.subplots(3, 1, sharex=True)
            gfdf.groupby(['url_etld1', 'profile_tag'])[field].sum().unstack(fill_value=0).cumsum().plot(ax=axen[0], title="Global-First Use", legend=False)
            sfdf.groupby(['url_etld1', 'profile_tag'])[field].sum().unstack(fill_value=0).cumsum().plot(ax=axen[1], title="Site-First Use", legend=False)
            mongo_df.groupby(['url_etld1', 'profile_tag'])[field].sum().unstack(fill_value=0).cumsum().plot(ax=axen[2], title="All Use", legend=False)
            handles, labels = axen[0].get_legend_handles_labels()
            fig.tight_layout()
            fig.legend(handles, labels, loc=(0, -0.01), ncol=4)
            fig.suptitle(f"Temporal Analysis of '{field}' ({dataset_label})")
            fig.savefig(f"{csv_stem}_temporal_{dataset_label}_{field}.pdf")
            plt.close(fig)
    

    return
    #--------------------------------------------------------------------------------
    # END TEST

    # computation: turn into graph counts
    c_adf = graph_counts_by_profile(adf)
    c_rdf = graph_counts_by_profile(rdf)
    c_tdf = graph_counts_by_profile(tdf)
    c_zdf = graph_counts_by_profile(zdf)

    # count all-profiles-same-count for each
    all_same_adf_mask = pred_all_same(c_adf)
    all_same_adf = sum(all_same_adf_mask)   # true == 1, false == 0; sum == number-of-trues
    all_same_rdf_mask = pred_all_same(c_rdf)
    all_same_rdf = sum(all_same_rdf_mask)
    all_same_tdf_mask = pred_all_same(c_tdf)
    all_same_tdf = sum(all_same_tdf_mask)
    all_same_zdf_mask = pred_all_same(c_zdf)
    all_same_zdf = sum(all_same_zdf_mask)
    print(f"Crawls with same-graph-count-across-all-profiles (all graphs): {all_same_adf:,}/{len(c_adf):,} ({(all_same_adf / len(c_adf)):%})")
    print(f"Crawls with same-graph-count-across-all-profiles (1p-only): {all_same_rdf:,}/{len(c_rdf):,} ({(all_same_rdf / len(c_rdf)):%})")
    print(f"Crawls with same-graph-count-across-all-profiles (3p-no-ad-only): {all_same_tdf:,}/{len(c_tdf):,} ({(all_same_tdf / len(c_tdf)):%})")
    print(f"Crawls with same-graph-count-across-all-profiles (3p-ad-only): {all_same_zdf:,}/{len(c_zdf):,} ({(all_same_zdf / len(c_zdf)):%})")

    # count number of graphs in balanced URLs vs total (for each)
    balanced_count_adf = c_adf[all_same_adf_mask].sum().sum()
    total_count_adf = c_adf.sum().sum()
    balanced_count_rdf = c_rdf[all_same_rdf_mask].sum().sum()
    total_count_rdf = c_rdf.sum().sum()
    balanced_count_tdf = c_tdf[all_same_tdf_mask].sum().sum()
    total_count_tdf = c_tdf.sum().sum()
    balanced_count_zdf = c_zdf[all_same_zdf_mask].sum().sum()
    total_count_zdf = c_zdf.sum().sum()
    print(f"Sum of graphs in balanced URLs (all graphs): {balanced_count_adf:,}/{total_count_adf:,} ({(balanced_count_adf / total_count_adf):%})")
    print(f"Sum of graphs in balanced URLs (1p-only): {balanced_count_rdf:,}/{total_count_rdf:,} ({(balanced_count_rdf / total_count_rdf):%})")
    print(f"Sum of graphs in balanced URLs (3p-no-ad-only): {balanced_count_tdf:,}/{total_count_tdf:,} ({(balanced_count_tdf / total_count_tdf):%})")
    print(f"Sum of graphs in balanced URLs (3p-ad-only): {balanced_count_zdf:,}/{total_count_zdf:,} ({(balanced_count_zdf / total_count_zdf):%})")

    # tally profile-with-most-graphs for each unbalanced crawl URL
    proclivity_sets = [
        ("all-graphs", c_adf, all_same_adf_mask),
        ("1p-only", c_rdf, all_same_rdf_mask),
        ("3p-no-ad", c_tdf, all_same_tdf_mask),
        ("3p-ad-only", c_zdf, all_same_zdf_mask),
    ]
    for name, df, mask in proclivity_sets:
        ppdf = df[~mask].transpose().apply(lambda x: x.sort_values().index[-1][:-1]).transpose().value_counts()
        ppdf_bottom = df[~mask].transpose().apply(lambda x: x.sort_values().index[0][:-1]).transpose().value_counts()
        ppdf_total = ppdf.sum()
        print(f"Most-prolific-profile-over-all-unbalanced-URLs ({name}):")
        for profile, count in ppdf.items():
            print(f"\t{profile:14} {count:8,}/{ppdf_total:,} ({count / ppdf_total:%})")
        print(f"Least-prolific-profile-over-all-unbalanced-URLs ({name}):")
        for profile, count in ppdf_bottom.items():
            print(f"\t{profile:14} {count:8,}/{ppdf_total:,} ({count / ppdf_total:%})")
    

    # experiment in identifying biggest steps in the url_etld1-cumsum-stairstep (for 3p-no-ad)
    """ total_graphs = tdf.groupby(['url_etld1', 'profile_tag']).url.count().unstack(fill_value=0)
    top_dogs = {profile: set(series.sort_values(ascending=False).iloc[:10].index) for profile, series in total_graphs.items()}
    
    top_intersection = reduce(lambda a, b: a & b, top_dogs.values())
    print("intersection:")
    print("\t" + "\n\t".join(top_intersection))
    top_union = reduce(lambda a, b: a | b, top_dogs.values())
    print("union - intersection:")
    print("\t" + "\n\t".join(top_union - top_intersection))

    tui = list(sorted(top_union))
    print(total_graphs.loc[tui])
    total_graphs.loc[tui].cumsum().plot()
    print(total_graphs.loc[tui].transpose().var().transpose())
    plt.show() """

    # identify top-variance in cross-profile-graph-counts by url_etld1 (3p-no-ad only)
    TOP_N = 5
    DF = tdf
    total_graphs = DF.groupby(['url_etld1', 'profile_tag']).url.count().unstack(fill_value=0)
    reject = total_graphs.transpose().var().sort_values(ascending=False).iloc[:TOP_N]
    print(reject)

    """ 
    all_the_things = DF[~DF.url_etld1.isin(reject.index)].groupby(['site_tag', 'profile_tag']).url.count().unstack(fill_value=0)
    print(all_the_things)
    all_the_things.cumsum().plot()
    plt.show() """

    # graph all of our metrics (and the total number of graphs) as cumulative-sum curves across all crawled URLs
    DF = DF[~DF.url_etld1.isin(reject.index)]
    cdf = DF.groupby(['site_tag', 'profile_tag']).total_nodes.count().unstack(fill_value=0).cumsum()
    ax = cdf.plot(title=f"Cumulative Graphs Across All Crawled URLs")
    ax.set_xticklabels([])
    fig = ax.get_figure()
    fig.autofmt_xdate(rotation=45)
    fig.tight_layout()
    fig.savefig(f"{csv_stem}_cumulative_GRAPHS.pdf")
    plt.close(fig)
    
    DF = DF.groupby(['site_tag', 'profile_tag']).sum()
    fields = DF.columns
    for field in fields:
        cdf = DF[field].unstack(fill_value=0).cumsum()
        ax = cdf.plot(title=f"Cumulative '{field}' Across All Crawled URLs")
        ax.set_xticklabels([])
        fig = ax.get_figure()
        fig.autofmt_xdate(rotation=45)
        fig.tight_layout()
        fig.savefig(f"{csv_stem}_cumulative_{field}.pdf")
        plt.close(fig)


if __name__ == "__main__":
    main(sys.argv)