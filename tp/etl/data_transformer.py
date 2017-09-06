import logging

import pandas as pd
import cx_Oracle as cx
from utils.db_utils import get_db_connection


class DataTransformer(object):
    """
    This class transforms the data as per the requirements.

    """

    def __init__(self, config, bid_number):
        self.log = logging.getLogger(__name__)
        self.config = config
        self.bid_number = bid_number
        self.db_engine = config.get('DATABASE', 'engine')

    def transform_data(self, df_tp20_bid,df_tp20_bid_shpr,df_tp20_svc_grp,df_tp20_ceiling_svc,df_tp20_shpr_svc,df_ttpsvgp,df_zone_weight,df_tp_accessorial):
        pass
