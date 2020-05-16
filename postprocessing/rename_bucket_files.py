#!/usr/bin/env python3

# data analysis
from datetime import datetime
import sys
import os
import shutil
import time
import csv
import argparse

import numpy as np
import pandas as pd

GIT_REPO = "https://tbd.com"
class GoogleBucketUtil:
    """
    This class postprocesses data collected during the personality network
    study, understanding personal value and objectives.
    """

    def __init__(self,csv_path,pretty=False,verbose=False):
        self.csv_path = csv_path
        self.pretty = pretty
        self.verbos = verbose

    def run(self):
        """MultilayerAnalsis.run()
        Main function that runs the postprocessing
        """

        print("\n----------------------------------------")
        print("Starting \'{}\'".format(self.__class__.__name__))
        print("----------------------------------------\n")

        files_in_path = os.listdir(self.csv_path)
        csv_paths = []
        for file in files_in_path:
            if file.endswith(".csv"):
                csv_paths.append(os.path.join(self.csv_path, file))

        for path in csv_paths:
            name = os.path.basename(path)
            try:
                for file in os.listdir(path):
                    if file.endswith(".csv"):
                        os.rename(os.path.join(path, file), os.path.join(self.csv_path, "moved_"+name))
                    #    print("old file"+os.path.join(path, file))
                    #    print("new file"+os.path.join(self.csv_path, "moved_"+name))
            except Exception as e:
                print("Failed to rename file in path {}: {}".format(path,e))
        
        #remove the directories
        for file in files_in_path:
            file_path = os.path.join(self.csv_path,file)
            if file.endswith(".csv"):
                if not file.startswith("moved_"):
                    # print("Does not start with moved_:\n{}".format(file))
                    try:
                        shutil.rmtree(file_path)
                    except Exception as e:
                        print("Failed to remove direcotry {}: {}".format(file_path,e))
        # now update files list and rename the files
        files_in_path = os.listdir(self.csv_path)
        for file in files_in_path:
            file_path = os.path.join(self.csv_path,file)
            if file.startswith("moved_"):
                try:
                    os.rename(file_path, file_path.replace('moved_', ''))
                except Exception as e:
                    print("Failed rename file {}: {}".format(file_path,e))

if __name__ == "__main__":
    prg_desc = 'Postprocessed personality project data. For more'
    prg_desc += 'information please refer to the git repository: '
    prg_desc += '{}'.format(GIT_REPO)

    parser = argparse.ArgumentParser(description=prg_desc)

    parser.add_argument('-i','--input',metavar='INPUT_PATH',
                        dest='csv_path',help='Path to data that needs processed.')
    parser.add_argument('-y','--pretty',action='store_true',default=False,
                        dest='pretty',help='Enables pretty output of all files.')

    args = parser.parse_args()

    go = GoogleBucketUtil(**vars(args))

    go.run()

          



