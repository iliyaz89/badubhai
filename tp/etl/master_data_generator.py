import numpy as np
import pandas as pd


class MasterDataGenerator(object):
    def create_prod_base_tables(self, home, bid_account_raw, bid_service_raw, zone_weight=None, tp20_svc_grp=None):
        bid_master_dd, product_master_sf = self.generate_base_tables(home, bid_account_raw, bid_service_raw)
        # Create temporary bid dummy data
        bid_master_dd['Bid_AE_Hist_Num_Bids'] = '1'
        bid_master_dd['Bid_Overall_Incentive'] = '1'
        bid_master_dd['Bid_Incentive_Freight'] = '1'

        # Create temporary product dummy data
        try:
            if zone_weight is None:
                # pre-process class will insert average values for columns with 'none' data. will happen in the
                # econ model step

                product_master_sf['0_0_5_lbs_pct'] = None
                product_master_sf['0_5625_1_lbs_pct'] = None
                product_master_sf['2_2_lbs_pct'] = None
                product_master_sf['3_3_lbs_pct'] = None
                product_master_sf['4_4_lbs_pct'] = None
                product_master_sf['5_5_lbs_pct'] = None
                product_master_sf['6_10_lbs_pct'] = None
                product_master_sf['11_15_lbs_pct'] = None
                product_master_sf['16_20_lbs_pct'] = None
                product_master_sf['21_25_lbs_pct'] = None
                product_master_sf['26_30_lbs_pct'] = None
                product_master_sf['31_40_lbs_pct'] = None
                product_master_sf['41_50_lbs_pct'] = None
                product_master_sf['51_75_lbs_pct'] = None
                product_master_sf['76_100_lbs_pct'] = None
                product_master_sf['101_535068_lbs_pct'] = None
                product_master_sf['2_pct'] = None
                product_master_sf['3_pct'] = None
                product_master_sf['4_pct'] = None
                product_master_sf['5_pct'] = None
                product_master_sf['6_pct'] = None
                product_master_sf['7_pct'] = None
                product_master_sf['8_pct'] = None
                product_master_sf['44_pct'] = None
                product_master_sf['45_pct'] = None
                product_master_sf['46_pct'] = None
            else:
                try:
                    tp20_svc_grp['SVM_TYP_CD'] = tp20_svc_grp['SVC_TYP_CD']
                    product_zw = self.zone_weight_table(home, zone_weight, tp20_svc_grp)
                    product_master_sf = product_master_sf.merge(product_zw, how='left')
                except:
                    # pre-process class will insert average values for columns with 'none' data. will happen in the
                    # econ model step

                    product_master_sf['0_0_5_lbs_pct'] = None
                    product_master_sf['0_5625_1_lbs_pct'] = None
                    product_master_sf['2_2_lbs_pct'] = None
                    product_master_sf['3_3_lbs_pct'] = None
                    product_master_sf['4_4_lbs_pct'] = None
                    product_master_sf['5_5_lbs_pct'] = None
                    product_master_sf['6_10_lbs_pct'] = None
                    product_master_sf['11_15_lbs_pct'] = None
                    product_master_sf['16_20_lbs_pct'] = None
                    product_master_sf['21_25_lbs_pct'] = None
                    product_master_sf['26_30_lbs_pct'] = None
                    product_master_sf['31_40_lbs_pct'] = None
                    product_master_sf['41_50_lbs_pct'] = None
                    product_master_sf['51_75_lbs_pct'] = None
                    product_master_sf['76_100_lbs_pct'] = None
                    product_master_sf['101_535068_lbs_pct'] = None
                    product_master_sf['2_pct'] = None
                    product_master_sf['3_pct'] = None
                    product_master_sf['4_pct'] = None
                    product_master_sf['5_pct'] = None
                    product_master_sf['6_pct'] = None
                    product_master_sf['7_pct'] = None
                    product_master_sf['8_pct'] = None
                    product_master_sf['44_pct'] = None
                    product_master_sf['45_pct'] = None
                    product_master_sf['46_pct'] = None
        except Exception:
            raise ValueError("Service group and zone weight creation error.")

        product_master_sf['Off_Net_Rev_wkly'] = 1
        product_master_sf['Overall_Incentive'] = 1
        product_master_sf['Incentive_Freight'] = 1
        product_master_sf['Act_Rev_Wkly'] = 1
        product_master_sf['Act_Vol_Wkly'] = 1

        return self.final_output(bid_master_dd, product_master_sf)

    def generate_base_tables(self, home, bid_account_raw, bid_service_raw):
        input_dir = home + '/data/inputs/'
        # trace('Loading data')

        # load data first
        try:
            svc_to_prod = pd.read_csv(input_dir + 'svc_to_prod.csv')
            # crossware_pld_bid_list = pd.read_csv(input_dir + 'Crossware_PLD_Bid_List.csv')
        except Exception, e:
            print e
            svc_to_prod = pd.DataFrame()
            # crossware_pld_bid_list = pd.DataFrame()

        # trace('Entering _clean_bid_account')
        bid_account = self.clean_bid_account(bid_account_raw)  # Do we need this step?
        # trace('Entering _clean_bid_service')
        bid_service = self.clean_bid_service(bid_service_raw, bid_account, svc_to_prod)  # Do we need this step?

        try:
            # trace('Entering _generate_product_initial')
            product_initial = self.generate_product_initial(bid_service)

            # trace('Entering _generate_bid_master')
            bid_master = self.generate_bid_master(bid_account, product_initial)

            # trace('Entering _generate_product_master')
            product_master = product_initial
        except Exception:
            raise ValueError("Bid service groups contain incorrect data")

        return bid_master, product_master

    # <editor-fold desc="Step 1: Base table prep from TP data">
    def clean_bid_account(self, bid_account_raw):
        # Don't process table twice
        if 'Proposed_Lag' in bid_account_raw.columns:
            return bid_account_raw  # already processed
        if 'RequestReason' not in bid_account_raw.columns:
            # trace('Request Reason is not in bid_account_raw, imputed all by 1')
            bid_account_raw['RequestReason'] = 1
        # Ensure that expected columns are present and select
        ba_cols = ['BidNumber', 'Status', 'StatusDate', 'ProposedDate', 'InitiationDate', 'Category', 'Region',
                   'District',
                   'RequestReason', 'LeadAccount']

        bid_account = bid_account_raw[ba_cols]

        # Coerce datetime columns
        # 'coerce' turns unparseable dts into NaT (some input dates out of bounds)
        dt_cols = ['InitiationDate', 'ProposedDate', 'StatusDate']
        bid_account[dt_cols] = bid_account[dt_cols]. \
            apply(lambda x: pd.to_datetime(x, errors='coerce'))

        # Find latest date
        lastDate = bid_account.StatusDate.max(skipna=True)

        # Transform bid_account_raw
        ba = bid_account  # alias for brevity
        ba['Proposed_Lag'] = (lastDate - ba.ProposedDate).dt.days

        # most bids are not explicitly rejected, but rarely are bids won > 90 days
        ba['Status_Updated'] = np.where((ba.Status == 'P') & (ba.Proposed_Lag > 90),
                                        'R', ba.Status)
        ba['ProposalBid_Flag'] = np.where(ba.Status == 'P', 1, 0)
        win_flag_map = {'A': '1',  # A = Accepted
                        'C': '1',  # C = Cancelled, previously accepted
                        'O': '1',  # O = Expired, previously accepted
                        'R': '0'}  # R = Rejected

        ba['Win_Flag'] = ba.Status.map(win_flag_map).fillna('Unclassified')
        ba['Win_Flag_Updated'] = ba.Status_Updated.map(win_flag_map).fillna('Unclassified')
        # Corercing RequestReason to int first (dirty data)
        ba['Request_Reason_Descr'] = pd.to_numeric(ba.RequestReason,
                                                   errors='coerce',
                                                   downcast='integer').map({1: 'RETENTION',
                                                                            2: 'PENETRATION',
                                                                            3: 'CONVERSION'}).fillna('Other')
        ba['Wks_since_Init'] = (ba.StatusDate - ba.InitiationDate).dt.days / 7.
        ba['Month_of_Bid'] = ba.InitiationDate.dt.month

        return ba

    def clean_bid_service(self, bid_service_raw, bid_account, svc_to_prod):
        # Don't process table twice
        if 'Product' in bid_service_raw.columns:
            return bid_service_raw  # already processed

        bid_service = bid_service_raw

        # Trim whitespace from Competitor column
        bid_service['Competitor'] = bid_service.Competitor.str.strip()

        # Attach product info to service table
        stp_cols = ['MVM_DRC_CD', 'SVM_TYP_CD', 'SVC_FEA_TYP_CD',
                    'Product']  # columns _required_ in svc_to_prod
        bid_service = pd.merge(bid_service, svc_to_prod[stp_cols],
                               on=['MVM_DRC_CD', 'SVM_TYP_CD', 'SVC_FEA_TYP_CD'],
                               how='left')  # possible to-dos given time: make this inner join, so that the service line is ignored; create error msg

        # Filter out unreliable data
        bid_service = bid_service[~bid_service.Product.isin(['GND_Unclassified',
                                                             'Unknown'])]

        return bid_service

    def generate_product_initial(self, bid_service):
        np.seterr(invalid='ignore')

        product_initial = bid_service[~pd.isnull(
            bid_service.Product)].copy()  # explicit copy to avoid warnings, could investigate avoiding copy in future
        product_initial['FedEx_Flag'] = np.where(product_initial.Competitor == 'FedEx', 1, 0)
        # trace('Starting main aggregation in _generate_product_initial')
        # Columns to aggregate
        # trace('Adding columns to aggregate')
        product_initial['List_Rev_Wkly'] = product_initial['List_Rev_PP'] * product_initial['Volume']

        product_initial.loc[product_initial['List_Rev_Wkly'] == 0, 'List_Rev_Wkly'] = 0.01

        product_initial['List_Rev_Freight_wkly'] = (product_initial['List_Rev_PP_Freight'] *
                                                    product_initial['Volume'])

        product_initial.loc[product_initial['List_Rev_Freight_wkly'] == 0, 'List_Rev_Freight_wkly'] = 0.01

        product_initial['Pct_FedEx'] = (product_initial['FedEx_Flag'] *
                                        product_initial['List_Rev_Wkly'])
        product_initial['Marginal_Cost_wkly'] = (product_initial['Marginal_Cost_PP'] *
                                                 product_initial['Volume'])
        product_initial['Total_Volume_wkly'] = product_initial['Volume']
        product_initial['Total_prior_Volume_wkly'] = product_initial['Prior_Volume']
        product_initial['Comp_Net_Rev_wkly'] = (product_initial['Comp_Net_Rev_PP'] *
                                                product_initial['Volume'])
        product_initial['Target_High_Rev_wkly'] = (product_initial['Target_High_Rev_PP'] *
                                                   product_initial['Volume'])
        product_initial['Target_Low_Rev_wkly'] = (product_initial['Target_Low_Rev_PP'] *
                                                  product_initial['Volume'])
        product_initial['PriorNetRevenue_wkly'] = product_initial['PriorNetRevenue']
        product_initial['PriorGrossRevenue_wkly'] = product_initial['PriorGrossRevenue']
        product_initial['Comp_Keyed_Rev_wkly'] = (product_initial['Comp_Keyed_Rev_PP'] *
                                                  product_initial['Volume'])
        product_initial['Comp_Est_Rev_wkly'] = (product_initial['Comp_Est_Rev_PP'] *
                                                product_initial['Volume'])
        product_initial['Density_Sum'] = (product_initial['True_Density'] *
                                          product_initial['List_Rev_Wkly'])

        # Sum weekly columns
        # trace('Summing weekly columns')
        grouped = product_initial.groupby(['BidNumber', 'Product'])
        prod_summ = grouped[['List_Rev_Wkly',
                             'List_Rev_Freight_wkly',
                             'Pct_FedEx',
                             'Marginal_Cost_wkly',
                             'Total_Volume_wkly',
                             'Total_prior_Volume_wkly',
                             'Comp_Net_Rev_wkly',
                             'Target_High_Rev_wkly',
                             'Target_Low_Rev_wkly',
                             'PriorNetRevenue_wkly',
                             'PriorGrossRevenue_wkly',
                             'Comp_Keyed_Rev_wkly',
                             'Comp_Est_Rev_wkly',
                             'Density_Sum']].sum()

        # Compute columns with denominators
        # trace('Computing columns with denominators')
        prod_summ['Pct_FedEx'] = prod_summ['Pct_FedEx'] / prod_summ['List_Rev_Wkly']

        # Compute additional columns
        # trace('Computing additional columns')
        prod_summ['TargetHigh_Inc'] = 1. - prod_summ['Target_High_Rev_wkly'] * 1. / prod_summ['List_Rev_Freight_wkly']
        prod_summ['TargetLow_Inc'] = 1. - prod_summ['Target_Low_Rev_wkly'] * 1. / prod_summ['List_Rev_Freight_wkly']
        prod_summ['Prior_Incentive'] = 1. - prod_summ['PriorNetRevenue_wkly'] * 1. / prod_summ['PriorGrossRevenue_wkly']
        prod_summ['Pct_New_Vol'] = np.maximum(
            1. - prod_summ['Total_prior_Volume_wkly'] * 1. / prod_summ['Total_Volume_wkly'], 0.)
        prod_summ['Gross_OR_Ratio'] = prod_summ['Marginal_Cost_wkly'] * 1. / prod_summ['List_Rev_Wkly']
        prod_summ['Pct_Rev_Freight'] = prod_summ['List_Rev_Freight_wkly'] * 1. / prod_summ['List_Rev_Wkly']
        prod_summ['Pct_Rev_Freight_Centered'] = prod_summ['Pct_Rev_Freight'] - np.mean(prod_summ['Pct_Rev_Freight'])
        prod_summ['Target_High_Incentive'] = 1. - prod_summ['Target_High_Rev_wkly'] * 1. / prod_summ[
            'List_Rev_Freight_wkly']
        prod_summ['True_Density'] = prod_summ['Density_Sum'] / prod_summ['List_Rev_Wkly']

        # possible to-dos given time: Don't hard code threshold - create ETL section of config.ini
        prod_summ['Comp_FedEx'] = np.where(prod_summ['Pct_FedEx'] > 0.95, 1, 0)
        prod_summ['Comp_3rdparty'] = np.where(prod_summ['Pct_FedEx'] < 0.2, 1, 0)

        # Comp_Net_Rev_wkly is after reality check on comp AE keyed and requires values be within 20pts Comp Est Weekly
        prod_summ['Comp_Incentive'] = np.maximum(
            1. - prod_summ.Comp_Net_Rev_wkly * 1. / prod_summ.List_Rev_Freight_wkly, 0.)
        prod_summ['Comp_AE_Keyed_Flag'] = np.where(prod_summ.Comp_Keyed_Rev_wkly > 0, 1, 0)
        prod_summ['Comp_AE_Keyed_Incentive'] = np.where(prod_summ.Comp_AE_Keyed_Flag == 1,
                                                        np.maximum(
                                                            1.0 - prod_summ.Comp_Keyed_Rev_wkly * 1. / prod_summ.List_Rev_Freight_wkly,
                                                            0.), np.NaN)

        # Compute MajorCompetitor
        # trace('Computing Major_Competitor')
        comp_sums = product_initial.groupby(['BidNumber', 'Product',
                                             'Competitor'])['List_Rev_Wkly'].sum()
        comp_ixs = comp_sums.groupby(level=[0, 1]).idxmax()
        comp_df = pd.DataFrame(comp_ixs.tolist(), columns=['BidNumber', 'Product', 'Competitor'], index=comp_ixs.index)
        prod_summ['Major_Competitor'] = comp_df['Competitor']

        # trace('Finished computing Major_Competitor')

        prod_summ = prod_summ.reset_index()

        # Computer Billing Tier
        cwt_density_bt_prod = grouped.aggregate({
            'Billing_Tier': lambda p: p.groupby(p).count().idxmax()}).reset_index()
        cwt_density_bt_prod = cwt_density_bt_prod[['BidNumber', 'Product', 'Billing_Tier']]

        # Merge Billing_Tier to product master
        prod_summ = pd.merge(prod_summ,
                             cwt_density_bt_prod,
                             on=['BidNumber', 'Product'],
                             how='left')

        product_initial = prod_summ.reset_index()

        # trace('Finished main aggregation in _generate_product_initial')

        # Bid-level revenue
        # trace('Computing bid-level revenue in _generate_product_initial')
        # Create Mode columns
        mode_df = product_initial[['BidNumber', 'Product', 'List_Rev_Wkly']].copy()  # explicit copy to avoid warning
        mode_df['Mode_GND_ListRev'] = np.where(
            mode_df.Product.isin(['GND_Com', 'GND_Resi', 'GND_USPS']),
            mode_df['List_Rev_Wkly'], 0.)
        mode_df['Mode_Air_ListRev'] = np.where(
            mode_df.Product.isin(['1DA', '2DA', '3DA']),
            mode_df['List_Rev_Wkly'], 0.)
        mode_df['Mode_Imp_Exp_ListRev'] = np.where(
            mode_df.Product.isin(['Import', 'Export']),
            mode_df['List_Rev_Wkly'], 0.)
        # Summarize and join Mode columns at bid level
        sum_cols = ['Mode_GND_ListRev', 'Mode_Air_ListRev', 'Mode_Imp_Exp_ListRev',
                    'List_Rev_Wkly']
        sums_df = mode_df.groupby('BidNumber')[sum_cols].sum().reset_index()
        sums_df = sums_df.rename(columns={'List_Rev_Wkly': 'Bid_List_Rev_Wkly'})  # rename before join
        product_initial = pd.merge(product_initial, sums_df,
                                   on='BidNumber')

        product_initial['Pct_Product_Rev'] = product_initial['List_Rev_Wkly'] / product_initial['Bid_List_Rev_Wkly']
        product_initial['Mode_Weight'] = np.where(product_initial.Product.isin(['GND_Com', 'GND_Resi', 'GND_USPS']),
                                                  product_initial['List_Rev_Wkly'] / product_initial[
                                                      'Mode_GND_ListRev'],
                                                  np.where(product_initial.Product.isin(['1DA', '2DA', '3DA']),
                                                           product_initial['List_Rev_Wkly'] / product_initial[
                                                               'Mode_Air_ListRev'],
                                                           product_initial['List_Rev_Wkly'] / product_initial[
                                                               'Mode_Imp_Exp_ListRev']))

        # Calculate product mode field:
        product_initial['Product_Mode'] = product_initial.Product.map({
            'GND_Com': 'GND',
            'GND_Resi': 'GND',
            'GND_USPS': 'GND',
            'GND_CWT': 'GND_CWT',
            '1DA': 'AIR',
            '2DA': 'AIR',
            '3DA': 'AIR',
            'Air_CWT': 'AIR_CWT',
            'Import': 'IE',
            'Export': 'IE'
        }).fillna('Unknown')  # possible to-dos given time: Default for anything should be GND_Com
        # Create unique key
        product_initial['BidNumberProduct'] = product_initial['BidNumber'] + '_' + product_initial['Product']

        # Bid_List_Rev_Wkly will become redundant when merging with bid_master
        product_initial.drop('Bid_List_Rev_Wkly', axis=1, inplace=True)
        return product_initial

    def generate_bid_master(self, bid_account, product_initial):
        bid_master = product_initial[~pd.isnull(product_initial.Product)]

        # trace('Starting main aggregation in _generate_bid_master')
        # Multiply some columns by revenue for summing
        bid_master['Pct_FedEx'] = bid_master['Pct_FedEx'] * bid_master['List_Rev_Wkly']
        # Creating mode percentage columns to sum
        bid_master['Dom_GND_Pct_Rev'] = np.where(bid_master.Product.isin(['GND_Com', 'GND_Resi', 'GND_USPS']),
                                                 bid_master.Pct_Product_Rev, 0)
        bid_master['Dom_AIR_Pct_Rev'] = np.where(bid_master.Product.isin(['1DA', '2DA', '3DA']),
                                                 bid_master.Pct_Product_Rev, 0)
        bid_master['Dom_GND_Resi_USPS_Pct_Rev'] = np.where(bid_master.Product.isin(['GND_Resi', 'GND_USPS']),
                                                           bid_master.Pct_Product_Rev, 0)
        bid_master['Imp_Exp_Pct_Rev'] = np.where(bid_master.Product.isin(['Import', 'Export']),
                                                 bid_master.Pct_Product_Rev, 0)
        bid_master['AIR_CWT_Pct_Rev'] = np.where(bid_master.Product.isin(['Air_CWT', ]), bid_master.Pct_Product_Rev, 0)
        bid_master['GND_CWT_Pct_Rev'] = np.where(bid_master.Product.isin(['GND_CWT', ]), bid_master.Pct_Product_Rev, 0)
        bid_master['Product_Concentration'] = bid_master['Pct_Product_Rev'] ** 2

        # Sum columns
        # trace('Summing columns')
        grouped = bid_master.groupby('BidNumber')
        bid_summ = grouped[['List_Rev_Wkly',
                            'List_Rev_Freight_wkly',
                            'Pct_FedEx',
                            'Marginal_Cost_wkly',
                            'Total_Volume_wkly',
                            'Total_prior_Volume_wkly',
                            'Comp_Net_Rev_wkly',
                            'Target_High_Rev_wkly',
                            'Target_Low_Rev_wkly',
                            'PriorNetRevenue_wkly',
                            'PriorGrossRevenue_wkly',
                            'Comp_Keyed_Rev_wkly',
                            'Comp_Est_Rev_wkly',
                            'Dom_GND_Pct_Rev',
                            'Dom_AIR_Pct_Rev',
                            'Dom_GND_Resi_USPS_Pct_Rev',
                            'Imp_Exp_Pct_Rev',
                            'AIR_CWT_Pct_Rev',
                            'GND_CWT_Pct_Rev',
                            'Product_Concentration']].sum()

        # Compute columns with denominators
        # trace('Computing columns with denominators')
        bid_summ['Pct_FedEx'] = bid_summ['Pct_FedEx'] / bid_summ['List_Rev_Wkly']

        # Compute additional columns
        # trace('Computing additional columns')
        bid_summ['Prior_Incentive'] = 1. - bid_summ['PriorNetRevenue_wkly'] * 1. / bid_summ['PriorGrossRevenue_wkly']
        bid_summ['Pct_New_Vol'] = np.maximum(
            1. - bid_summ['Total_prior_Volume_wkly'] * 1. / bid_summ['Total_Volume_wkly'], 0.)
        bid_summ['Product_ReqReason_Desc'] = np.where(bid_summ['Pct_New_Vol'] > 0.6, 'CONVERSION_Prod',
                                                      np.where(bid_summ['Pct_New_Vol'] <= 0.2, 'RETENTION_Prod',
                                                               'PENETRATION_Prod'))
        bid_summ['Gross_OR_Ratio'] = bid_summ['Marginal_Cost_wkly'] * 1. / bid_summ['List_Rev_Wkly']
        bid_summ['Pct_Rev_Freight'] = bid_summ['List_Rev_Freight_wkly'] * 1. / bid_summ['List_Rev_Wkly']

        # possible to-dos given time: hard code should go into INI file
        bid_summ['Comp_FedEx'] = np.where(bid_summ['Pct_FedEx'] > 0.95, 1, 0)
        bid_summ['Comp_3rdparty'] = np.where(bid_summ['Pct_FedEx'] < 0.2, 1, 0)
        bid_summ['Dom_GND_Centric_Flag'] = np.where(bid_summ['Dom_GND_Pct_Rev'] > 0.8, 1, 0)
        bid_summ['Dom_AIR_Centric_Flag'] = np.where(bid_summ['Dom_AIR_Pct_Rev'] > 0.6, 1, 0)
        bid_summ['Dom_GND_Resi_USPS_Centric_Flag'] = np.where(bid_summ['Dom_GND_Resi_USPS_Pct_Rev'] > 0.6, 1, 0)
        bid_summ['Imp_Exp_Centric_Flag'] = np.where(bid_summ['Imp_Exp_Pct_Rev'] > 0.4, 1, 0)

        # print bid_master[['BidNumber', 'Major_Competitor','List_Rev_Wkly']]

        # This is equivalent to
        # trace('Computing Major_Competitor')
        comp_sums = bid_master.groupby(['BidNumber', 'Major_Competitor'])['List_Rev_Wkly'].sum()
        comp_ixs = comp_sums.groupby(level=0).idxmax()
        comp_df = pd.DataFrame(comp_ixs.tolist(),
                               columns=['BidNumber', 'Major_Competitor'],
                               index=comp_ixs.index)

        bid_summ['Major_Competitor'] = comp_df['Major_Competitor']
        # trace('Finished computing Major_Competitor')

        bid_master = bid_summ.reset_index()

        # trace('Finished main aggregation in _generate_bid_master')

        # Join bid_master with bid_account
        # Join bid_master with bid_account
        bid_master = pd.merge(bid_master, bid_account,
                              on='BidNumber', how='left')

        bid_master['Comp_Rev_Keyed_Flag'] = np.where(pd.isnull(bid_master.Comp_Keyed_Rev_wkly), 0,
                                                     np.where((bid_master.Comp_Keyed_Rev_wkly == 0) &
                                                              (bid_master.Comp_Est_Rev_wkly > 1),
                                                              0, 1))

        # 1.2 and 0.8 factors aligns with TP 1.0 factors
        bid_master['Comp_Rev_Keyed_Higher_Flag'] = np.where(
            bid_master.Comp_Keyed_Rev_wkly > bid_master.Comp_Est_Rev_wkly * 1.2,
            1, 0)
        bid_master['Comp_Rev_Keyed_Lower_Flag'] = np.where(
            bid_master.Comp_Keyed_Rev_wkly < bid_master.Comp_Est_Rev_wkly * 0.8,
            1, 0)
        bid_master['List_Rev_Wkly_LessThan200'] = np.where(bid_master.List_Rev_Wkly < 200,
                                                           1, 0)
        # Add prefix "Bid" to all bid-level features
        bid_master.rename(columns=lambda col: 'Bid_' + col if col != 'BidNumber' else col, inplace=True)

        return bid_master

    def final_output(self, bid_master_dd, product_master_sf):
        bid_master_final = bid_master_dd
        # Join product master with bid master
        product_master_final = pd.merge(product_master_sf, bid_master_final,
                                        on='BidNumber', how='inner')

        # Drop duplicated columns
        bid_master_final = bid_master_final.loc[:, ~bid_master_final.columns.duplicated()]  # Keep the first column
        product_master_final = product_master_final.loc[:,
                               ~product_master_final.columns.duplicated()]  # Keep the first column

        ### Imputations
        # Prior Incentive
        product_master_final.loc[(product_master_final['Prior_Incentive'].isnull()) &
                                 (product_master_final['Bid_Request_Reason_Descr'] == 'CONVERSION'),
                                 'Prior_Incentive'] = 0
        bid_master_final.loc[(bid_master_final['Bid_Prior_Incentive'].isnull()) &
                             (bid_master_final['Bid_Request_Reason_Descr'] == 'CONVERSION'),
                             'Bid_Prior_Incentive'] = 0

        return product_master_final
