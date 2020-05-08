###############################################################################
# This script tests the parser key idea for the  IPIP-NEO 120 Item Key 
# spreadsheet.
###############################################################################import os
import sys
import csv

# what we want
# code_dict = {
#     "CASE": ([1,6],float)
#     "SEX": ([7, 7],float)
#     "AGE": ([8, 9],float)
#     "SEC": ([10, 11],float)
#     "MIN": ([12, 13],float)
#     "HOUR": ([14, 15],float)
#     "DAY": ([16, 17],float)
#     "MONTH": ([18, 19],float)
#     "YEAR": ([20, 22],float)
#     "COUNTRY": ([23, 31],float)
#     "SCORES": ([32, 151],float)
# }


import os
import sys
import csv
import json
from pprint import pprint

basepath = "/home/christnp/Development/e6895/data"
parkey = os.path.join(basepath,"IPIP-NEO/IPIP-NEO-ParseKey.csv")
data = os.path.join(basepath,"IPIP-NEO/IPIP120.dat")


# start function
with open(data, newline='',mode='r') as data_f, open(parkey,mode='r') as parkey_f:
    reader = csv.DictReader(parkey_f)
    p_dict = {}
    for row in reader:
        # element = "(({},{}),{})".format(row['Start'],row['End'],row['Format'])
        # p_dict[row['Variable']] = element
        # p_dict[row['Variable']] = ([int(row['Start']),int(row['End'])],str(row['Format']))
        p_dict[row['Variable']] = [int(row['Start']),int(row['End'])]
        # print("{},({},{}),{}".format(row['Variable'],row['Start'],row['End'],row['Format']))
    
    # tmp0 = [int(p_dict[x][0][0]) for x in p_dict]
    # tmp1 = [p_dict[x][0][1] for x in p_dict]
    # print(tmp0)

    # print(p_dict["CASE"])
    reader = csv.DictReader(data_f)
    # record = []
    for row in reader:
        # tmp = [row[int(p_dict[x][0][0])-1:int(p_dict[x][0][1])] for x in p_dict]
        # tmp = [row[p_dict[x][0]-1:p_dict[x][0]] for x in p_dict]
        # row.map(lambda line: [line[p_dict[x][0]-1:p_dict[x][1]] for x in p_dict])
        # print(tmp)
        print(row[0])


          


print("Done.")