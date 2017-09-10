import logging
import numpy as np
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

    def transform_data(self, df_tp20_bid, df_tp20_bid_shpr, df_tp20_svc_grp, df_tp20_shpr_svc, df_ttpsvgp):
        # Lead Account shipper service.
        dfp_tp20_shpr_svc_leadaccount = df_tp20_shpr_svc.copy()
        dfp_tp20_shpr_svc_leadaccount['LIST_REV'] = dfp_tp20_shpr_svc_leadaccount['SHR_PJT_WVL_QY'] * \
                                                    dfp_tp20_shpr_svc_leadaccount['SHR_PJT_GRS_RPP_A']
        dfp_tp20_shpr_svc_leadaccount = dfp_tp20_shpr_svc_leadaccount.groupby(by=['NVP_BID_NR', 'SHR_AC_NR']).sum()[
            ['LIST_REV']]
        dfp_tp20_shpr_svc_leadaccount = dfp_tp20_shpr_svc_leadaccount.reset_index()
        dfp_tp20_shpr_svc_leadaccount = dfp_tp20_shpr_svc_leadaccount.sort_values(by=['NVP_BID_NR', 'LIST_REV'],
                                                                                  ascending=[1, 0])
        dfp_tp20_shpr_svc_leadaccount[['NVP_BID_NR', 'SHR_AC_NR']]

        # Prior revenue shipper service
        df_tp20_shpr_svc_priorrev = df_tp20_shpr_svc.copy()
        df_tp20_shpr_svc_priorrev['PRIOR_GRS_REV'] = df_tp20_shpr_svc_priorrev['SHR_PJT_WVL_QY'] * \
                                                     df_tp20_shpr_svc_priorrev[
                                                         'SHR_PRR_GRS_RVN_A']
        df_tp20_shpr_svc_priorrev['PRIOR_NET_REV'] = df_tp20_shpr_svc_priorrev['SHR_PJT_WVL_QY'] * \
                                                     df_tp20_shpr_svc_priorrev[
                                                         'SHR_PRR_NET_RVN_A']
        df_tp20_shpr_svc_priorrev = df_tp20_shpr_svc_priorrev.groupby(by=['NVP_BID_NR', 'SVC_GRP_NR']).agg(
            {'PRIOR_GRS_REV': np.sum, 'PRIOR_NET_REV': np.sum})
        df_tp20_shpr_svc_priorrev = df_tp20_shpr_svc_priorrev.reset_index()
        df_tp20_shpr_svc_priorrev = df_tp20_shpr_svc_priorrev[
            ['NVP_BID_NR', 'SVC_GRP_NR', 'PRIOR_GRS_REV', 'PRIOR_NET_REV']]

        # Region, District and Lead account
        df_region = df_tp20_bid_shpr[['NVP_BID_NR', 'REG_NR']].groupby(['NVP_BID_NR']).apply(
            lambda g: g.groupby('REG_NR').count().idxmax())
        df_region.columns = ['REGION']
        df_region = df_region.reset_index()

        df_district = df_tp20_bid_shpr[['NVP_BID_NR', 'DIS_NR']].groupby(['NVP_BID_NR']).apply(
            lambda g: g.groupby('DIS_NR').count().idxmax())
        df_district.columns = ['DISTRICT']
        df_district = df_district.reset_index()

        df_leadaccount = dfp_tp20_shpr_svc_leadaccount[['NVP_BID_NR', 'SHR_AC_NR']].groupby(['NVP_BID_NR']).apply(
            lambda g: g.groupby('SHR_AC_NR').count().idxmax())
        df_leadaccount.columns = ['LEAD_ACCOUNT']
        df_leadaccount = df_leadaccount.reset_index()

        # Merge bid info and shipper info.
        dp_tp20_bid_shpr_combined = pd.merge(pd.merge(df_tp20_bid, df_leadaccount), pd.merge(df_region, df_district),
                                             on='NVP_BID_NR')
        del df_region, df_district, df_leadaccount

        # TP BID table.
        columns = {'NVP_BID_NR': 'BID_NUMBER',
                   'NVP_BID_STS_CD': 'STATUS',
                   'BID_STS_EFF_DT': 'STATUS_DATE',
                   'NVP_BID_PRP_DT': 'PROPOSED_DATE',
                   'NVP_BID_INI_DT': 'INITIATION_DATE',
                   'NVP_BID_CGY_TYP_CD': 'CATEGORY',
                   }

        df_tp_bid_table = dp_tp20_bid_shpr_combined[
            ['NVP_BID_NR', 'NVP_BID_STS_CD', 'BID_STS_EFF_DT', 'NVP_BID_PRP_DT', 'NVP_BID_INI_DT', 'NVP_BID_CGY_TYP_CD',
             'REGION', 'DISTRICT', 'LEAD_ACCOUNT']]

        # TODO: Need for mapping the categories.
        # Category to ['Not Known','National','Major','Key','Small'] ??


        # Merge variables to ISA SVC tables

        df_ttpsvgp.drop('PND_STS_CD', 1, inplace=True)
        max_factor = df_ttpsvgp['TRG_PSE_FCR_NR'].max()
        df_ttpsvgp = df_ttpsvgp[df_ttpsvgp.TRG_PSE_FCR_NR == max_factor]
        df_ttpsvgp = df_ttpsvgp.groupby(['NVP_BID_NR', 'SVC_GRP_NR']).agg('max')
        df_ttpsvgp = df_ttpsvgp.reset_index()

        df_tp20_svc_grp = df_tp20_svc_grp.merge(df_ttpsvgp, how='left')

        ## Merge Prior Net/Gross Rev
        df_tp20_svc_grp = df_tp20_svc_grp.merge(df_tp20_shpr_svc_priorrev, how='left')

        # Create FedEx competitor identifier
        COMPETITOR_CATEGORY = {15: 'FedEx',
                               16: 'FedEx',
                               60: 'FedEx',
                               83: 'FedEx'}

        # SVC Table.
        df_tp_bid_svc_table = df_tp20_svc_grp[['NVP_BID_NR','SVC_GRP_NR','MVM_DRC_CD','SVC_TYP_CD','PKG_CHA_TYP_CD',
                                               'SVC_FEA_TYP_CD','CALC_CUBE_PACKAGE_DENSITY','CPE_CRR_NR','BID_UPS_PJT_WVL_QY',
                                               'BID_CUS_PRR_WVL_QY','NVP_BID_FRT_GRR_A','UPS_PRJ_GRS_RPP_A','BID_MRG_CPP_A',
                                               'BID_FA_CPP_A','CPE_SVC_RPP_A','CPE_ETM_RPP_A','SVC_GRP_TRG_PSE_A','SVC_TRG_LOW_RNG_A','SVC_TRG_HI_RNG_A']]
        # Competitor Categories
        df_tp_bid_svc_table['COMPETITOR'] = df_tp20_svc_grp['CPE_CRR_NR'].map(COMPETITOR_CATEGORY).fillna('Other')
        df_tp_bid_svc_table['Comp_Net_Rev_PP']= np.where(df_tp20_svc_grp['CPE_SVC_RPP_A'] == 0,
                                                         df_tp20_svc_grp['CPE_ETM_RPP_A'],
                                                         df_tp20_svc_grp['CPE_SVC_RPP_A'])

        df_tp_bid_svc_table['PRIOR_GRS_REV'] =  df_tp20_svc_grp['PRIOR_GRS_REV'] / 13
        df_tp_bid_svc_table['PRIOR_NET_REV'] =  df_tp20_svc_grp['PRIOR_NET_REV'] / 13

        # TODO: If required, rename the columns.

        df_tp20_shpr_svc = df_tp20_shpr_svc[['NVP_BID_NR','SVC_GRP_NR','RA_TRI_NR']]
        df_tp_bid_svc_table = df_tp_bid_svc_table.merge(df_tp20_shpr_svc, how="left")
        df_tp_bid_svc_table = df_tp_bid_svc_table.rename(columns={'RA_TRI_NR':'Billing_Tier'})

        return df_tp_bid_table, df_tp_bid_svc_table