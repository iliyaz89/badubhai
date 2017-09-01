import logging

import pandas as pd
from sqlalchemy import create_engine


class DataFetcher(object):
    """
    This class loads data from the following views.

    """

    def __init__(self, config, bid_number):
        self.log = logging.getLogger(__name__)
        self.config = config
        self.bid_number = bid_number
        self.db_engine = config.get('DATABASE', 'engine')

    def fetch_data(self):

        connection = create_engine(self.db_engine)
        schema = self.config.get('DATABASE', 'schema')
        print(connection)
        if connection:

            # Load the views in an order.

            # 1. tp_bid
            q1 = "SELECT * FROM V_TP20_BID where NVP_BID_NR = '%s' "%self.bid_number
            df_tp20_bid = pd.read_sql(q1, con=connection)


            # 2. tp_bid_shpr
            q2 = "SELECT * FROM V_TP20_BID_SHPR_INFO WHERE NVP_BID_NR = '%s' "%self.bid_number
            df_tp20_bid_shpr = pd.read_sql(q2, con=connection).sort_values(by=['NVP_BID_NR', 'SHR_AC_NR'], ascending=[1, 0])

            # 3. tp_svc_grp
            q3 = "SELECT * FROM V_TP20_SERVICE_GROUP WHERE NVP_BID_NR = '%s' "%self.bid_number
            df_tp20_svc_grp = pd.read_sql(q3, con=connection)

            # 4. tp_ceiling_svc
            q4 = "SELECT * FROM V_TP20_CEILING_SERVICES WHERE NVP_BID_NR = '%s' "%self.bid_number
            df_tp20_ceiling_svc = pd.read_sql(q4, con=connection)

            # 5. tp20_shpr_svc
            q5 = "SELECT * FROM V_TP20_SHPR_SVC_VOL_REV WHERE NVP_BID_NR = '%s'"%self.bid_number
            df_tp20_shpr_svc = pd.read_sql(q5, con=connection)
            # TODO: Filter out the columsn not required.


            print(df_tp20_shpr_svc.shape)


            print(df_tp20_shpr_svc.shape)
            #
            #
            #
            # tp20_shpr_svc = pd.read_sql(query, con=connection).loc[:, ['NVP_BID_NR', 'SVC_GRP_NR', 'SHR_AC_NR', 'RA_TRI_NR',
            #                                                    'SHR_PJT_WVL_QY',  # Current Quantity
            #                                                    'SHR_PJT_GRS_RPP_A',  # Current Price PP
            #                                                    'SHR_PRR_GRS_RVN_A',  # Prior Gross PP
            #                                                    'SHR_PRR_NET_RVN_A']]  # Prior Net PP
            #
            # # ttpsvgp
            # query = "SELECT NVP_BID_NR, SVC_GRP_NR, PND_STS_CD, CPE_ETM_RPP_A, SVC_GRP_TRG_PSE_A, SVC_TRG_LOW_RNG_A," \
            #         " SVC_TRG_HI_RNG_A, TRG_PSE_FCR_NR FROM TTPSVGP WHERE NVP_BID_NR = '" + self.bid_number + "'"
            # ttpsvgp = pd.read_sql(query, con=connection)
            #
            #
            #
            # #zone_weight
            # query = "SELECT NVP_BID_NR,SVC_GRP_NR,SVC_GRP_SUF_NR,DEL_ZN_NR,WGT_MS_UNT_TYP_CD, " \
            #         "WGT_CGY_WGY_QY,round(ADJ_PKG_VOL_QY ,10) as PKGBOL," \
            #         "(CASE WGT_MS_UNT_TYP_CD WHEN 'OZ' " \
            #         "THEN cast(WGT_CGY_WGY_QY as DECIMAL(9,2)) / 16.0 " \
            #         "ELSE cast(WGT_CGY_WGY_QY as DECIMAL(9,2)) END) " \
            #         "as WEIGHT FROM V_TP20_ZONE_WGT_VOL_DIST WHERE NVP_BID_NR = '" + self.bid_number + \
            #         "' AND DEL_ZN_NR != 'ALL'"
            #
            # zone_weight = pd.read_sql(query, con=connection)
            #
            # # tncvcel
            # query = "SELECT DISTINCT C.*, D.MVM_DRC_CD, D.SVC_TYP_CD, D.SVC_FEA_TYP_CD FROM " \
            #         "(SELECT A.* FROM " \
            #         "(SELECT NVP_BID_NR, SVC_GRP_NR, RCM_NCV_QY, NCV_MIN_QY, NCV_MAX_QY FROM TNCVCEL " \
            #         "WHERE NVP_BID_NR = '" + self.bid_number + "') A " \
            #                                               "INNER JOIN V_TP20_CEILING_SERVICES B " \
            #                                               "ON A.NVP_BID_NR = B.NVP_BID_NR AND A.SVC_GRP_NR = B.SVC_GRP_NR) C " \
            #                                               "INNER JOIN V_TP20_SERVICE_GROUP D ON C.NVP_BID_NR = D.NVP_BID_NR AND C.SVC_GRP_NR = D.SVC_GRP_NR"
            # tncvcel = pd.read_sql(query, con=connection)
            #
            # #tp_accessorial
            # query = "SELECT * FROM V_TP20_ACCESSORIAL WHERE NVP_BID_NR = '" + self.bid_number + "'"
            # tp_accessorial = pd.read_sql(query, con=connection)


        else:
            self.log.critical('Unable to access the databse.')
