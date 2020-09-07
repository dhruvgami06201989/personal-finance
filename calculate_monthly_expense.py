import os
import sys
import datetime
import json
import glob
import pandas as pd
from file_path_generator import FilePathGenerator
import multiprocessing as mp


class ProcessStatement:
    def __init__(self, fpg):
        self.fpg = fpg
        self.general_categ_dict = None
        self.specific_categ_dict = None

    def update_mapping(self, log_txt):
        # Two types of word mapping files for allocating categories to your transactions
        # (Specific word maps are directly copied from your past history if you want to run past x
        # months of transactions. You will have to work on putting this together)
        mapping_filename = self.fpg.mapping_file('Category Mapping')
        general_mapping_dict_file = self.fpg.mapping_file('Stored General Maps')
        specific_mapping_dict_file = self.fpg.mapping_file('Stored Specific Maps')

        if os.path.isfile(mapping_filename):
            map_file_update_datetime = datetime.datetime.fromtimestamp(os.path.getmtime(mapping_filename))
        else:
            log_txt.write(">> No Mapping file found!!\n")
            sys.exit(">> No Mapping file found!!")

        update_flag = 0
        if os.path.isfile(general_mapping_dict_file) and os.path.isfile(specific_mapping_dict_file):
            dict_file_latest_datetime = datetime.datetime.fromtimestamp(os.path.getmtime(general_mapping_dict_file))
            if map_file_update_datetime > dict_file_latest_datetime:
                update_flag = 1
        else:
            update_flag = 1

        if update_flag == 1:
            log_txt.write(">> Mapping Dict files need to be updated\n")
            general_categ_map_df = pd.read_excel(mapping_filename, sheet_name="General")
            specific_categ_map_df = pd.read_excel(mapping_filename, sheet_name="Specific")

            general_categ_dict = dict(zip(general_categ_map_df['Description'], general_categ_map_df['Category']))

            with open(general_mapping_dict_file, 'w') as dict_file:
                json.dump(general_categ_dict, dict_file)

            specific_categ_dict = dict(zip(specific_categ_map_df['Description'], specific_categ_map_df['Category']))

            with open(specific_mapping_dict_file, 'w') as dict_file:
                json.dump(specific_categ_dict, dict_file)

        else:
            log_txt.write(">> No need to update mapping dict files\n")
            with open(general_mapping_dict_file, 'r') as dict_file:
                general_categ_dict = json.load(dict_file)

            with open(specific_mapping_dict_file, 'r') as dict_file:
                specific_categ_dict = json.load(dict_file)

        self.general_categ_dict = general_categ_dict
        self.specific_categ_dict = specific_categ_dict

        log_txt.write(">> Mapping dict files updated successfully!\n")

    def get_raw_bank_statements(self):
        # You will have to change your bank_ls and account_ls for each bank here
        bank_ls = ['Chase', 'DCU', 'Fifth Third']
        raw_statement_df_dict = {}
        for bank in bank_ls:
            file_ls = glob.glob(self.fpg.bank_statements(bank) + '/*.csv')
            if bank == 'Chase':
                account_ls = ['3133', '2703', '3103', '1862']
            elif bank == 'DCU':
                account_ls = ['Checking', 'Savings']
            elif bank == 'Fifth Third':
                continue
            for each_accnt in account_ls:
                key = bank + '-' + each_accnt
                account_file_ls = [x for x in file_ls if each_accnt in x]
                acct_file_latest = max(account_file_ls, key=os.path.getctime)
                # ---- This might or might not be required for your bank ----
                if bank == 'DCU':
                    each_acct_df = pd.read_csv(acct_file_latest, header=3)
                    each_acct_df['Memo'] = each_acct_df['Memo'].fillna(each_acct_df['Description'])
                    each_acct_df['Amount Debit'] = each_acct_df['Amount Debit'].fillna(0)
                    each_acct_df['Amount Credit'] = each_acct_df['Amount Credit'].fillna(0)
                    each_acct_df.loc[:, 'Amount'] = each_acct_df['Amount Debit'] + each_acct_df['Amount Credit']
                else:
                    each_acct_df = pd.read_csv(acct_file_latest, index_col=False)
                each_acct_df.loc[:, 'Credit/Debit'] = each_acct_df['Amount'].apply(lambda x:
                                                                                   'Credit' if x > 0 else 'Debit')
                raw_statement_df_dict.update({key: each_acct_df})
        return raw_statement_df_dict

    def allocate_category(self, statement_df, bank, accnt_str):
        # Change your banks and account_str for your accounts
        if bank == 'Chase':
            description_col = 'Description'
            if accnt_str == '3133' or accnt_str == '2703':
                post_date_col = 'Transaction Date'
                type_col = 'Type'
            elif accnt_str == '3103' or accnt_str == '1862':
                post_date_col = 'Posting Date'
                type_col = 'Details'
        elif bank == 'DCU':
            description_col = 'Memo'
            post_date_col = 'Date'
            type_col = 'Description'

        key = bank + '-' + accnt_str

        exception_dict = {'ATM CHECK DEPOSIT 04/01 2943 RICHLAND AVE LOUISVILLE KY': 'Tax/Interest'}
        updated_statement_dict = {}
        statement_updated_df = pd.DataFrame()
        for idx, row in statement_df.iterrows():
            substring_match = [substring for substring in self.general_categ_dict.keys() if substring.upper() in
                               row[description_col].upper()]
            if substring_match:
                if len(substring_match) == 1:
                    category = self.general_categ_dict[substring_match[0]]
                    mapping_found = 'General-Single'
                else:
                    print("Multiple matching substrings found for: " + row[description_col] +
                          " in General Mapping. Category assigned: " + self.general_categ_dict[substring_match[0]])
                    categories = [self.general_categ_dict[x] for x in substring_match]
                    category = categories[0]
                    mapping_found = 'General-Multiple: ' + ", ".join(categories) + ". Selected: " + category
            else:
                substring_match = [substring for substring in self.specific_categ_dict.keys() if
                                   substring.upper() in row[description_col].upper()]
                if substring_match:
                    if len(substring_match) == 1:
                        category = self.specific_categ_dict[substring_match[0]]
                        mapping_found = 'Specific-Single'
                    else:
                        print("Multiple matching substrings found for: " + row[description_col] +
                              " in Specific Mapping. Category assigned: " + self.specific_categ_dict[substring_match[0]])
                        categories = [self.specific_categ_dict[x] for x in substring_match]
                        category = categories[0]
                        mapping_found = 'Specific-Multiple: ' + ", ".join(categories) + ". Selected: " + category
                else:
                    print("No matching Category for: " + str(row[description_col]))
                    category = 'Other'
                    mapping_found = "None. Selected " + category

            if row[description_col] in list(exception_dict.keys()):
                category = exception_dict[row[description_col]]
                mapping_found = 'Exception case mapping'

            if bank == 'DCU':
                exception_dict_dcu = {'TRANSFER TO LOAN 141': 'Auto Loan', 'TRANSFER FROM ACCT 1': 'Bank Transfers',
                                      'TRANSFER TO ACCT 2': 'Bank Transfers'}
                if row[type_col] in list(exception_dict_dcu.keys()):
                    category = exception_dict_dcu[row[type_col]]
                    mapping_found = 'DCU Exception case mapping'

            if statement_updated_df.empty:
                statement_updated_df = pd.DataFrame(
                    data=[[row[post_date_col], row[description_col], row[type_col], row['Amount'], category,
                           mapping_found, row['Credit/Debit'], bank, accnt_str]],
                    columns=['Date', 'Description', 'Type', 'Amount', 'Category', 'Mapping Reason', 'Credit/Debit',
                             'Bank', 'Acct Num'])
                updated_statement_dict.update({key: statement_updated_df})
            else:
                statement_updated_df = statement_updated_df.append(pd.DataFrame(
                    data=[[row[post_date_col], row[description_col], row[type_col], row['Amount'], category,
                           mapping_found, row['Credit/Debit'], bank, accnt_str]],
                    columns=['Date', 'Description', 'Type', 'Amount', 'Category', 'Mapping Reason', 'Credit/Debit',
                             'Bank', 'Acct Num']))
                updated_statement_dict.update({key: statement_updated_df})
        print("debug")
        return updated_statement_dict


class MultiprocessStatements:
    def __init__(self):
        self.result = {}

    def collect_results(self, result):
        self.result.update(result)


def main(run_date, statement_mon_year):
    run_date_str = run_date.strftime("%Y-%m-%d")
    fpg = FilePathGenerator(run_date)
    log_file = fpg.output_file('Log') + '\\' + 'Log_' + run_date_str + '.log'
    log_txt = open(log_file, 'w')
    print("-----------Running for" + statement_mon_year + "-------------")
    log_txt.write("-----------Running for" + statement_mon_year + "-------------\n")
    statement_processor = ProcessStatement(fpg)
    print(">> Updating mapping file.....")
    log_txt.write(">> Updating mapping file.....\n")
    statement_processor.update_mapping(log_txt)
    cores = mp.cpu_count()
    pool = mp.Pool(processes=cores)
    statement_multiprocessor = MultiprocessStatements()

    log_txt.write(">> Get Raw bank statements....\n")
    print(">> Get Raw bank statements....")
    raw_statement_df_dict = statement_processor.get_raw_bank_statements()
    print(">> Raw Bank Statements fetched successfully")
    print(">> Updating categories for all bank statements using parallel processing")
    log_txt.write(">> Raw Bank Statements fetched successfully\n")
    log_txt.write(">> Updating categories for all bank statements using parallel processing\n")
    for key, each_acct_raw_df in raw_statement_df_dict.items():
        bank = key.split('-')[0]
        account = key.split('-')[1]
        # ------------------- TEMPORARY DEBUGGING -----------------
        # if key == 'DCU-Checking' or key == 'DCU-Savings':
        #     bank = key.split('-')[0]
        #     account = key.split('-')[1]
        #     raw_acct_df = raw_statement_df_dict[key]
        #     each_updated_bank_df_dict = statement_processor.allocate_category(raw_acct_df, bank, account)
        #     statement_multiprocessor.collect_results(each_updated_bank_df_dict)
        # ---------------------------------------------------------
        pool.apply_async(statement_processor.allocate_category, args=(each_acct_raw_df, bank, account),
                         callback=statement_multiprocessor.collect_results)
    pool.close()
    pool.join()
    print(">> Updated all statements successfully!")
    log_txt.write(">> Updated all statements successfully!\n")

    updated_bank_stat_dict = statement_multiprocessor.result
    combined_new_bank_statement = pd.DataFrame()
    for each_bank_acct_df in updated_bank_stat_dict.values():
        combined_new_bank_statement = combined_new_bank_statement.append(each_bank_acct_df, sort=False)

    combined_new_bank_statement.sort_values(by='Date', ascending=False)
    combined_new_bank_statement = combined_new_bank_statement.drop_duplicates()

    all_statement_combined_filename = fpg.output_file('Combined Statement All')
    if os.path.isfile(all_statement_combined_filename):
        combined_new_bank_statement.to_csv(fpg.output_file('Combined Statement New', statement_mon_year), index=False)
    else:
        combined_new_bank_statement.to_csv(fpg.output_file('Combined Statement All'), index=False)

    log_txt.write(">> Run Completed on: " + run_date_str)
    print(">> Run Completed on: " + run_date_str)
    log_txt.close()


def combine_statement_all(file_name_generator, statement_mon_year):
    all_statement_combined_filename = file_name_generator.output_file('Combined Statement All')
    combined_new_statement_filename = file_name_generator.output_file('Combined Statement New', statement_mon_year)
    if os.path.isfile(all_statement_combined_filename) and os.path.isfile(combined_new_statement_filename):
        all_statement_combined_df = pd.read_csv(all_statement_combined_filename)
        combined_new_bank_statement = pd.read_csv(combined_new_statement_filename)
        all_statement_combined_df = all_statement_combined_df.append(combined_new_bank_statement, sort=False)
        all_statement_combined_df = all_statement_combined_df.drop_duplicates()
        all_statement_combined_df.to_csv(all_statement_combined_filename, index=False)
        print(">> Combined Statement file generated successfully!")
    else:
        print(">> Combined Statement file not generated!")


if __name__ == "__main__":
    run_date = datetime.datetime.today()
    statement_mon_year = 'Aug_2020'
    # main(run_date, statement_mon_year)
    # print("End of Run")

    file_generator = FilePathGenerator(run_date)
    combine_statement_all(file_generator, statement_mon_year)
