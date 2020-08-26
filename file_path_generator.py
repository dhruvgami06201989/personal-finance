import os
import sys
import datetime


class FilePathGenerator:
    def __init__(self, run_date):
        self.working_dir = r'C:\Dhruv\Project Data\Personal Finance'
        self.log_dir = self.working_dir + '\\' + 'log'
        self.run_date = run_date

    def bank_statements(self, bank):
        if bank == 'Chase':
            file_path = self.working_dir + '\\' + 'Bank Statements' + '\\' + 'Chase'
        if bank == 'DCU':
            file_path = self.working_dir + '\\' + 'Bank Statements' + '\\' + 'DCU'
        if bank == 'Fifth Third':
            file_path = self.working_dir + '\\' + 'Bank Statements' + '\\' + 'Fifth Third'
        os.makedirs(file_path, exist_ok=True)
        return file_path

    def mapping_file(self, file_type):
        file_path = self.working_dir + '\\' + 'Internal'
        if file_type == 'Category Mapping':
            file_name = file_path + '\\' + 'Category Mapping.xlsx'
            return file_name
        if file_type == 'Stored General Maps':
            file_name = file_path + '\\' + 'General Mapping.json'
            return file_name
        if file_type == 'Stored Specific Maps':
            file_name = file_path + '\\' + 'Specific Mapping.json'
            return file_name

    def output_file(self, file_type):
        if file_type == 'Combined Statement New':
            file_name = self.working_dir + '\\' + 'Combined_Statement_' + self.run_date.strftime("%Y-%m-%d") + '.csv'
            return file_name
        if file_type == 'Combined Statement All':
            file_name = self.working_dir + '\\' + 'Combined_Statement_All.csv'
            return file_name
        if file_type == 'Log':
            log_dir = self.working_dir + '\\' + 'Logs'
            os.makedirs(log_dir, exist_ok=True)
            return log_dir

