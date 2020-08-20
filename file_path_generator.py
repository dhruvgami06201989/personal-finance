import os
import sys
import datetime


class FilePathGenerator:
    def __init__(self):
        self.working_dir = r'C:\Dhruv\Project Data\Personal Finance'

    def bank_statements(self, bank):
        if bank == 'Chase':
            file_path = self.working_dir + '\\' + 'Bank Statements' + '\\' + 'Chase'
        if bank == 'DCU':
            file_path = self.working_dir + '\\' + 'Bank Statements' + '\\' + 'DCU'
        if bank == 'Fifth Third':
            file_path = self.working_dir + '\\' + 'Bank Statements' + '\\' + 'Fifth Third'
        os.makedirs(file_path, exist_ok=True)
        return file_path

    def mapping_file(self, type):
        file_path = self.working_dir + '\\' + 'Internal'
        if type == 'Category Mapping':
            file_name = file_path + '\\' + 'Category Mapping.xlsx'
            return file_name
        if type == 'Stored General Maps':
            file_name = file_path + '\\' + 'General Mapping.json'
            return file_name
        if type == 'Stored Specific Maps':
            file_name = file_path + '\\' + 'Specific Mapping.json'
            return file_name
