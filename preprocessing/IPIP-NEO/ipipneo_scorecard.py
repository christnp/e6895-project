###############################################################################
# RAN  ON 4/5/2020
#
# This script converts the  IPIP-NEO 120 Item Key spreadsheet into a new, more
# precise format.
###############################################################################
import os
import sys
import csv
import json
from pprint import pprint

basepath = "/home/christnp/Development/e6895/data"
item_key = os.path.join(basepath,"IPIP-NEO/IPIP-NEO-ItemKey.csv")
scorecard = os.path.join(basepath,"IPIP-NEO/IPIP-NEO-Scorecard.csv")


# start function
with open(item_key, newline='',mode='r') as ifile, open(scorecard,mode='w') as ofile:
    fieldnames = ['ID','Key','Sign','Adjusted','Facet','Description']
    csv_out = csv.DictWriter(ofile, fieldnames=fieldnames)
    csv_out.writeheader()

    reader = csv.DictReader(ifile)
    record = []
    for row in reader:
        if row['Short#']:
            # record.append({
            #     'Item#':row['Short#'],
            #     'Key':row['Key'],
            #     'Sign':row['Sign'][0], # example, key 'N5', sign '-N5' => key 'N5', sign '-'
            #     'Adjusted': 'True', # has sign already been appled?
            #     'Facet':row['Facet'],
            #     'Item':row['Item'],
            # })

            csv_out.writerow({
                'ID':int(row['Short#']), # Item ID / #
                'Key':row['Key'],
                'Sign':row['Sign'][0], # example, key 'N5', sign '-N5' => key 'N5', sign '-'
                'Adjusted': 'True', # has sign already been appled?
                'Facet':row['Facet'],
                'Description':row['Item'], # user definable, typically Item description
            })


# scorecard_json = json.load(short_keys)
# pprint(scorecard_json)
# for item in short_keys:
#     print(item)
print("Done.")