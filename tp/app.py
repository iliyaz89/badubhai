#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""[application description here]"""

__appname__ = "Target Pricing Model"
__author__ = "Iliaz"
__version__ = "0.0.1"

import logging

log = logging.getLogger(__name__)

import argparse
import ConfigParser
from etl.data_fetcher import DataFetcher


# -- Code Here --
def set_up_opt_parser():
    parser = argparse.ArgumentParser(description='TP 2.0 Engine.')
    parser.add_argument('-v', '--verbose', type=int, default=2)
    parser.add_argument('-q', '--quiet', type=int, default=0)

    # ETL
    parser.add_argument('-etl', '--run-etl', action='store_true', default=True)

    args = parser.parse_args()

    # Set up clean logging to stderr
    log_levels = [logging.CRITICAL, logging.ERROR, logging.WARNING,
                  logging.INFO, logging.DEBUG]

    args.verbose = min(args.verbose - args.quiet, len(log_levels) - 1)
    args.verbose = max(args.verbose, 0)
    logging.basicConfig(level=log_levels[args.verbose],
                        format='%(levelname)s: %(message)s')

    return args


def load_app_config():
    config = ConfigParser.ConfigParser()
    config.readfp(open('tp20_config.ini'))
    return config


if __name__ == '__main__':
    args = set_up_opt_parser()
    config = load_app_config()

    if args.run_etl:
        log.info('Running ETL script.')

        bid_number = 'P260030994'
        data_fetcher = DataFetcher(config, bid_number)
        data_fetcher.fetch_data()
