import logging

import pandas as pd
import cx_Oracle as cx
from utils.db_utils import get_db_connection
import os
from utils.db_utils import get_db_connection


class DataLoader(object):
    """
    This class pushes the data as per the requirements.

    """

    def __init__(self, config, bid_number):
        self.log = logging.getLogger(__name__)
        self.config = config
        self.bid_number = bid_number
        self.db_engine = config.get('DATABASE', 'engine')

    def get_randomizer(self, num):
        randomizer_file = os.path.join(self.config['TP20_INPUT_PATH'], 'randomizer_file.csv')
        randomizer_df = pd.read_csv(randomizer_file)
        return randomizer_df.iloc[int(num)]['On'] == True

    def load_data(self, response, tp_accessorial):
        acy_table = response[response['Product_Mode'] == 'ACY']
        prod_table = response[response['Product_Mode'] != 'ACY']
        query = ""
        sql = ""

        last = int(self.bid_number[-1])
        if self.get_randomizer(last):
            # check available accessorials
            if not acy_table.empty:
                acy_table = acy_table.drop('SVC_GRP_NR', axis=1)  # remove due to blank causing issues
                acy_table = acy_table.merge(tp_accessorial,
                                            how='inner', on=['MVM_DRC_CD', 'SVC_FEA_TYP_CD', 'SVM_TYP_CD',
                                                             'ASY_SVC_TYP_CD', 'PKG_CHA_TYP_CD', 'PKG_ACQ_MTH_TYP_CD'])

            # add regular products
            try:

                connection = get_db_connection()
                cursor = connection.cursor()

                # remove accessorial from list
                acc_svcs = tp_accessorial.SVC_GRP_NR.unique()
                prod_table = prod_table[~prod_table['SVC_GRP_NR'].isin(acc_svcs)]

                # once update once per service group
                prod_table = prod_table[
                    ['SVC_GRP_NR', 'Incentive_Freight', 'Target_Low', 'Target_High']].drop_duplicates()

                for index, row in prod_table.iterrows():
                    # formatting at %.3f truncates the value to 3 decimal places
                    svc_grp_num = row["SVC_GRP_NR"]
                    incentive = round(row["Incentive_Freight"], 3)
                    min_inc = round(row["Target_Low"], 3)
                    max_inc = round(row["Target_High"], 3)

                    query = "UPDATE TNCVCEL " \
                            "SET RCM_NCV_QY = " + str(incentive) + ", NCV_MIN_QY = " + str(
                        min_inc) + ", NCV_MAX_QY = " + str(max_inc) + \
                            " WHERE NVP_BID_NR = '" + str(self.bid_number) + "' AND SVC_GRP_NR = '" + str(svc_grp_num) \
                            + "' AND NCV_DTR_DAT_TYP_CD = 'P'"

                    cursor.execute(query)
                    cursor.execute('COMMIT')

                    cursor.close()
                    connection.close()

                    # logger.info(str(prod_table.shape[0]) + " rows available to update to TNCVCEL")
            except Exception, e:
                raise RuntimeError(
                    'Error 4.1: Oracle DB cannot be updated with query: \n' + query + 'with error: ' + str(e))
