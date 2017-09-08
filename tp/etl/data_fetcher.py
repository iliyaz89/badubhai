import logging

import pandas as pd
import cx_Oracle as cx
from utils.db_utils import get_db_connection


class DataFetcher(object):
    """
    This class loads data from the following views.

    """

    def __init__(self, config, bid_number):
        self.log = logging.getLogger(__name__)
        self.config = config
        self.bid_number = bid_number

    def fetch_data(self):

        connection = get_db_connection()
        if connection:

            # Load the views in an order.

            # 1. tp_bid
            q1 = "SELECT * FROM V_TP20_BID where NVP_BID_NR = '%s' " % self.bid_number
            df_tp20_bid = pd.read_sql(q1, con=connection)

            # 2. tp_bid_shpr
            q2 = "SELECT * FROM V_TP20_BID_SHPR_INFO WHERE NVP_BID_NR = '%s' " % self.bid_number
            df_tp20_bid_shpr = pd.read_sql(q2, con=connection).sort_values(by=['NVP_BID_NR', 'SHR_AC_NR'],
                                                                           ascending=[1, 0])

            # 3. tp_svc_grp
            q3 = "SELECT * FROM V_TP20_SERVICE_GROUP WHERE NVP_BID_NR = '%s' " % self.bid_number
            df_tp20_svc_grp = pd.read_sql(q3, con=connection)

            # 4. tp_ceiling_svc
            q4 = "SELECT * FROM V_TP20_CEILING_SERVICES WHERE NVP_BID_NR = '%s' " % self.bid_number
            df_tp20_ceiling_svc = pd.read_sql(q4, con=connection)

            # 5. tp20_shpr_svc
            q5 = "SELECT * FROM V_TP20_SHPR_SVC_VOL_REV WHERE NVP_BID_NR = '%s'" % self.bid_number
            df_tp20_shpr_svc = pd.read_sql(q5, con=connection)
            # TODO: Filter out the columns not required.

            # 6. ttpsvgp
            q6 = "SELECT NVP_BID_NR, SVC_GRP_NR, PND_STS_CD, CPE_ETM_RPP_A, SVC_GRP_TRG_PSE_A, SVC_TRG_LOW_RNG_A, " \
                 "SVC_TRG_HI_RNG_A, TRG_PSE_FCR_NR FROM TTPSVGP WHERE NVP_BID_NR = '%s' " % self.bid_number
            df_ttpsvgp = pd.read_sql(q6, con=connection)

            # 7. zone_weight
            q7 = "SELECT NVP_BID_NR,SVC_GRP_NR,SVC_GRP_SUF_NR,DEL_ZN_NR,WGT_MS_UNT_TYP_CD, " \
                 "WGT_CGY_WGY_QY, PKGBOL," \
                 "(CASE WGT_MS_UNT_TYP_CD WHEN 'OZ' " \
                 "THEN cast(WGT_CGY_WGY_QY as DECIMAL(9,2)) / 16.0 " \
                 "ELSE cast(WGT_CGY_WGY_QY as DECIMAL(9,2)) END) " \
                 "as WEIGHT FROM V_TP20_ZONE_WGT_VOL_DIST WHERE NVP_BID_NR = '%s' AND DEL_ZN_NR != 'ALL'" % self.bid_number
            #
            df_zone_weight = pd.read_sql(q7, con=connection)

            # tncvcel
            q8 = "SELECT DISTINCT C.*, D.MVM_DRC_CD, D.SVC_TYP_CD, D.SVC_FEA_TYP_CD FROM " \
                 "(SELECT A.* FROM " \
                 "(SELECT NVP_BID_NR, SVC_GRP_NR, RCM_NCV_QY, NCV_MIN_QY, NCV_MAX_QY FROM TNCVCEL " \
                 "WHERE NVP_BID_NR = '%s') A " \
                 "INNER JOIN V_TP20_CEILING_SERVICES B " \
                 "ON A.NVP_BID_NR = B.NVP_BID_NR AND A.SVC_GRP_NR = B.SVC_GRP_NR) C " \
                 "INNER JOIN V_TP20_SERVICE_GROUP D ON C.NVP_BID_NR = D.NVP_BID_NR AND C.SVC_GRP_NR = D.SVC_GRP_NR" % self.bid_number
            df_tncvcel = pd.read_sql(q8, con=connection)

            # tp_accessorial
            q9 = "SELECT * FROM V_TP20_ACCESSORIAL WHERE NVP_BID_NR = '%s' "%self.bid_number
            df_tp_accessorial = pd.read_sql(q9, con=connection)

            import joblib
            import tempfile
            import os
            outfile = os.path.join(tempfile.gettempdir(),'%s.data'%self.bid_number)
            joblib.dump([df_tp20_bid,df_tp20_bid_shpr,df_tp20_svc_grp,df_tp20_ceiling_svc,df_tp20_shpr_svc,df_ttpsvgp,df_zone_weight,df_tp_accessorial],outfile,compress=5)
            if(self.validate_data(df_tp20_bid,df_tp20_bid_shpr,df_tp20_svc_grp,df_tp20_ceiling_svc,df_tp20_shpr_svc,df_ttpsvgp,df_zone_weight,df_tp_accessorial)):
                return df_tp20_bid,df_tp20_bid_shpr,df_tp20_svc_grp,df_tp20_ceiling_svc,df_tp20_shpr_svc,df_ttpsvgp,df_zone_weight,df_tp_accessorial
            else:
                self.log.critical('Data validation for the bid number %s failed.'%(self.bid_number))
        else:
            self.log.critical('Unable to access the databse.')



    def validate_data(self, df_tp20_bid,df_tp20_bid_shpr,df_tp20_svc_grp,df_tp20_ceiling_svc,df_tp20_shpr_svc,df_ttpsvgp,df_zone_weight,df_tp_accessorial):
        # TODO: Add validation to check if the dataframes are empty?
        return True