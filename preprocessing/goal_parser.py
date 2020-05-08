
#!/usr/bin/python3

from bs4 import BeautifulSoup
import csv 
import json
# import codecs
import pandas as pd
from pprint import pprint
import re
import sys
import os


# create the json object, using topics as keys
def parseGoals(shape=None,weights=[.2,.2,.2,.2,.2],verbose=False):
    '''
    Returns JSON object (TODO: Pandas dataframe) with goal categories as columns given the weights
    for each of the 5 personalities. Given the goal categories, we assume the 
    priorities for each personality as follows:
        'Extraversion' - social,adventure,career,personal,financial,spiritual,knowledge,health
        'Agreeableness' - spiritual,financial,health,social,adventure,knowledge,career,personal
        'Conscientiousness' - career,financial,personal,health,knowledge,spiritual,adventure,social
        'Neuroticism' - health,personal,spiritual,financial,adventure,career,social,knowledge
        'Openness' - knowledge,adventure,spiritual,personal,financial,social,career,health

    * reference for determining priorities https://www.verywellmind.com/the-big-five-personality-dimensions-2795422
    '''
    dpath = "../data/Goals/" # path to goal data folder
    fnames = ["100 Life Goals - 1.html","100 Life Goals - 2.html","100 Life Goals - 3.html"]
    topics = ['financial', 'spiritual','social','knowledge','personal',
                'health','adventure','career','unknown']
    if not shape:
        pass

    # goal_json = {}
    goal_json = []
    # for topic in topics:
        # goal_json[topic] = []
    # source = codecs.open(file, 'r', 'utf-8')
    for fname in fnames:
        path = dpath+fname
        if verbose:
            print("Parsing {}...".format(path))
    
        with open(path, 'r') as f:
            # read in the html file
            source = f.read()
            # parse the HTML
            parsed = BeautifulSoup(source,'lxml')
            # temporary variables
            topics = []
            goals = []
            cur_topic = ''
            # recursively parse the HTML with the following assumptions: 
            #   (1) "goal topics" encapsulated in <h2> tags
            #   (2) "goals" encapsulated in <p> tags
            for child in parsed.recursiveChildGenerator():
                
                if child.name:
                    if(child.name == "h2"):
                        if verbose:
                            print(">> {}".format(child.text))
                        topics.append(child.text)
                        cur_topic = child.text.lower()   

                    if(child.name == 'p'):
                        tmp = re.sub(r"(\d+)\. ", "", child.text, 0, re.MULTILINE) 
                        # then condense to single spaced words
                        tmp = re.sub(r"\s+"," ",tmp)
                        # then replace '\n' with empty string
                        goal = tmp.strip().replace(r"\n", "")
                        # print(repr(goal))
                        goals.append((cur_topic,goal))
                        try:
                            # goal_json[cur_topic].append(goal)
                            goal_json.append({"topic":cur_topic,"goal":goal})
                        except:
                            if verbose:
                                print("!! Unknown goal topic: {}".format(cur_topic))
                            # goal_json['unknown'].append(goal)
                            goal_json.append({"topic":cur_topic,"goal":goal})

        if verbose:
            # user display
            pprint(goal_json)

        if verbose:
            print("Total goals: {}".format(len(goal_json)))

    return goal_json

    # TODO: randomly assign goals to user profile info, perhaps with some weights

def createDataFrame(jsonobject,columns=None):
    # create dataframe for data storage
    if columns:
        return pd.DataFrame(jsonobject, columns = columns) 
    else:
        return pd.DataFrame(jsonobject) 

def savetoJSON(jsonobject, filename="data.json"):
    # writing to json file 
    with open(filename, 'w') as outfile:
        outfile.write(json.dumps(jsonobject, indent=2))
        # json.dump(jsonobject, outfile)

def savetoCSV(listdata,filename,header=None): 
    print(listdata)
    sys.exit()
    if not header:
        pass
        # header = listdata
    # writing to csv file 
    with open(filename, 'w') as csvfile: 
        # creating a csv dict writer object 
        writer = csv.DictWriter(csvfile, fieldnames = header) 
        # writing headers (field names) 
        writer.writeheader() 
        # writing data rows 
        writer.writerows(listdata) 
  
      
def main(): 
    basepath="../data/Goals/"
    goalsjson=os.path.join(basepath,"goals.json")
    goalscsv=os.path.join(basepath,"goals.csv")
    # load rss from web to update existing xml file 
    goals = parseGoals() # for display, use verbose=True
    savetoJSON(goals,goalsjson)
    # savetoCSV(goals,goalscsv)

    df = createDataFrame(goals)
    print(goalscsv)
    res = df.to_csv(goalscsv, index = False, header=True)
    if res:
        print(res)
    print(df.info())

      
if __name__ == "__main__": 
  
    # calling main function 
    main() 