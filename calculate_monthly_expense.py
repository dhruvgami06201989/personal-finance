import os
import sys
import datetime
import json
import glob
import pandas as pd
from file_path_generator import FilePathGenerator


class ProcessStatement:
    def __init__(self, fpg):
        self.fpg = fpg
        self.general_categ_dict = None
        self.specific_categ_dict = None

    def update_mapping(self):
        mapping_filename = self.fpg.mapping_file('Category Mapping')
        general_mapping_dict_file = self.fpg.mapping_file('Stored General Maps')
        specific_mapping_dict_file = self.fpg.mapping_file('Stored Specific Maps')

        if os.path.isfile(mapping_filename):
            map_file_update_datetime = datetime.datetime.fromtimestamp(os.path.getmtime(mapping_filename))
        else:
            sys.exit(">> No Mapping file found!!")

        update_flag = 0
        if os.path.isfile(general_mapping_dict_file) and os.path.isfile(specific_mapping_dict_file):
            dict_file_latest_datetime = datetime.datetime.fromtimestamp(os.path.getmtime(general_mapping_dict_file))
            if map_file_update_datetime > dict_file_latest_datetime:
                update_flag = 1
        else:
            update_flag = 1

        # update_flag = 0
        # file_update_date = datetime.datetime.fromtimestamp(os.path.getmtime(mapping_filename)).date()
        # if file_update_date >= (datetime.datetime.today().date() - datetime.timedelta(days=30)):
        #     update_flag = 1

        if update_flag == 1:
            general_categ_map_df = pd.read_excel(mapping_filename, sheet_name="General")
            specific_categ_map_df = pd.read_excel(mapping_filename, sheet_name="Specific")
            # uniq_general_categ = general_categ_map_df['Category'].unique().tolist()
            # uniq_specf_categ = general_categ_map_df['Category'].unique().tolist()

            general_categ_dict = dict(zip(general_categ_map_df['Description'], general_categ_map_df['Category']))

            with open(general_mapping_dict_file, 'w') as dict_file:
                json.dump(general_categ_dict, dict_file)

            specific_categ_dict = dict(zip(specific_categ_map_df['Description'], specific_categ_map_df['Category']))

            with open(specific_mapping_dict_file, 'w') as dict_file:
                json.dump(specific_categ_dict, dict_file)

        else:
            with open(general_mapping_dict_file, 'r') as dict_file:
                general_categ_dict = json.load(dict_file)

            with open(specific_mapping_dict_file, 'r') as dict_file:
                specific_categ_dict = json.load(dict_file)

        self.general_categ_dict = general_categ_dict
        self.specific_categ_dict = specific_categ_dict

        # return general_categ_dict, specific_categ_dict

    def allocate_category(self, statement_df, accnt_str):
        if accnt_str == '3133' or '2703' or '3103':
            descr_col_name = 'Description'
            categ_col_name = 'Category'

        chase_3133_updated_df = pd.DataFrame()
        for idx, row in statement_df.iterrows():
            substring_match = [substring for substring in self.general_categ_dict.keys() if substring.upper() in
                               row['Description'].upper()]
            if substring_match:
                if len(substring_match) == 1:
                    category = self.general_categ_dict[substring_match[0]]
                else:
                    print("Multiple matching substrings found for: " + row['Description'] +
                          " in General Mapping. Category assigned: " + self.general_categ_dict[substring_match[0]])
                    category = self.general_categ_dict[substring_match[0]]
            else:
                substring_match = [substring for substring in self.specific_categ_dict.keys() if
                                   substring.upper() in row['Description'].upper()]
                if substring_match:
                    if len(substring_match) == 1:
                        category = self.specific_categ_dict[substring_match[0]]
                    else:
                        print("Multiple matching substrings found for: " + row['Description'] +
                              " in Specific Mapping. Category assigned: " + self.specific_categ_dict[substring_match[0]])
                        category = self.specific_categ_dict[substring_match[0]]
                else:
                    print("No matching Category for: " + str(row['Description']))
                    category = 'Other'

            if chase_3133_updated_df.empty:
                chase_3133_updated_df = pd.DataFrame(
                    data=[[row['Post Date'], row['Description'], row['Type'], row['Amount'], category]],
                    columns=['Post Date', 'Description', 'Type', 'Amount', 'Category'])
            else:
                chase_3133_updated_df = chase_3133_updated_df.append(pd.DataFrame(
                    data=[[row['Post Date'], row['Description'], row['Type'], row['Amount'], category]],
                    columns=['Post Date', 'Description', 'Type', 'Amount', 'Category']))
        print("debug")


def main():
    fpg = FilePathGenerator()
    statement_processor = ProcessStatement(fpg)
    statement_processor.update_mapping()

    chase_file_ls = glob.glob(fpg.bank_statements('Chase') + '/*.csv')
    chase_3133_file_ls = [x for x in chase_file_ls if '3133' in x]
    chase_2703_file_ls = [x for x in chase_file_ls if '2703' in x]
    chase_3103_file_ls = [x for x in chase_file_ls if '3103' in x]
    chase_3133_file_latest = max(chase_3133_file_ls, key=os.path.getctime)
    chase_2703_file_latest = max(chase_2703_file_ls, key=os.path.getctime)
    chase_3103_file_latest = max(chase_3103_file_ls, key=os.path.getctime)

    chase_3133_df = pd.read_csv(chase_3133_file_latest)
    chase_2703_df = pd.read_csv(chase_2703_file_latest)
    chase_3103_df = pd.read_csv(chase_3103_file_latest)

    statement_processor.allocate_category(chase_3133_df, '3133')
    print("debug")

    chase_df_ls = [chase_3133_df, chase_2703_df, chase_3103_df]
    # for each_chase_df in chase_df_ls:

    return


if __name__ == "__main__":
    # file_path_gen = FilePathGenerator()
    # statement_processor = ProcessStatement(file_path_gen)
    # statement_processor.update_mapping()
    main()
    print("End of Run")
