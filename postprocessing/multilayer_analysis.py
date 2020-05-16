#!/usr/bin/env python3

# data analysis
from datetime import datetime
import sys
import os
import shutil
import time
import csv
import argparse
import re

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from itertools import zip_longest
from functools import reduce

GIT_REPO = "https://tbd.com"
class MultilayerAnalysis:
    """
    This class postprocesses data collected during the personality network
    study, understanding personal value and objectives.
    """

    def __init__(self,csv_path,graph_path=None,pretty=False,verbose=False):
        self.csv_path = csv_path
        self.graph_path = graph_path
        self.pretty = pretty
        self.verbose = verbose
    
    def getNodes(self,nodeDF,bestNodes,verbose=False):
        # we assume bestNodes is a list of tuples [(id,comp_wgt),...]
        # where comp_wgt is optional
        uuids = []
        wghts = []
        try:
            uuids,wghts= zip(*bestNodes)
            # print("uuids = {}".format(uuids))
            # print("wghts = {}".format(wghts))
        except:
            # print("Looks like bestNodes does not include weights. No Worries!")
            uuids = [x[0] for x in bestNodes]
            # print("uuids = {}".format(uuids))

        bestDF = nodeDF.loc[nodeDF['id'].isin(uuids)]
        if wghts:
            bestDF['bestcomp_wght'] = wghts

        return uuids,bestDF
        

    def getTopN(self,bestComp,commDF,N=5,id="id",values=False,display=False,verbose=False):
        # reach back into to the community factors and select the highlight components
        tmp = commDF.sort_values(by=bestComp,ascending=False)
        # tmp = commDF.sort_values(by=bestComp,ascending=True)
        # tmp = commDF.sort_values(by=bestComp)
        # pick out the top N nodes
        if values:
            topN = tmp.head(N)[[id,bestComp]]
        else:
            topN = tmp.head(N)[[id]]

        # return [t for tn in topN.values for t in tn]
        return list(map(tuple, topN.values))

    def highlightComponents(self,scoredDF,nodes=0,mask=None,output=None,pretty=None):
        
        if not mask:
            # default: high everything, except neu
            mask = [("OPN",1),("CSN",1),("EXT",1),("AGR",1),("NEU",-1)]

        # we have a profile (masks)
        maskDF = pd.DataFrame(mask,columns=["layer","mask"])

        # get the max and min of each row (i.e., layer); we use this as our 
        # most ideal candidate from the set
        bestDF = scoredDF.copy()
        bestDF['max'] = bestDF.filter(regex='s_').values.max(axis=1)
        bestDF['min'] = bestDF.filter(regex='s_').values.min(axis=1)
        # lets merge (or join) our masks into the 
        bestDF = pd.merge(bestDF,maskDF, on=["layer"], how='left')

        # now our best fit logic... if mask is > 0, we chose the max set value;
        # if, however, mask < 0 then we choose the min set value; else zero
        bestDF['best'] = bestDF['mask']
        bestDF.loc[bestDF['mask'] > 0, 'best'] =  bestDF.loc[bestDF['mask'] > 0, 'max']
        bestDF.loc[bestDF['mask'] < 0, 'best'] =  bestDF.loc[bestDF['mask'] < 0, 'min']
        # get rid of everything except the best profile and layer columns
        bestDF = bestDF[['layer','best']]
        
        # lets join the best possible profile given the set with our scored DF
        scoredDF = scoredDF.merge(bestDF,on="layer")

        # here we calculate the square error for each component compared to
        # the ideal or best profile (i.e, looking for best match)
        def mse(x, y):
            return (x - y)**2

        # select the component(s) with the best match(es)
        fitKey = {}
        for column in scoredDF.filter(regex='s_').columns:
            mseCol = "mse_" + column.split("_")[1]
            scoredDF[mseCol] = mse(scoredDF["best"], scoredDF[column])
            fitKey[column] = mse(scoredDF["best"], scoredDF[column]).values.sum()
        
        # return the component that minimizes the square error
        winner = min(fitKey, key=fitKey.get)
        # return scoredDF[["layer", min(fitKey, key=fitKey.get)]]
        return winner.split("_")[1], scoredDF[["layer", winner]]


    def buildString(self,str_fmt):
        return str_fmt[0].format(str_fmt[1])

    def communityPlot(self,scoredDF,rank,nodes,factor,title=None,sub=[],output=False,display=False,verbose=False):
        # sns.set(style="darkgrid")
        sns.set(style="whitegrid")
        if not title:
            title = 'Personality Level as a Function of Factor {} Tensor Component'.format(factor)
       
        ymax = np.ceil(scoredDF.filter(regex='s_').values.max())
        ymin = np.floor(scoredDF.filter(regex='s_').values.min())
        sns.set_palette("Set1", n_colors=scoredDF.shape[0], desat=.5)
        # if rank is >=3, then restrict columns to 3. Else, let it equal the rank
        if rank >= 3 and rank != 4:
            fig_col = 3
        elif rank == 4:
            fig_col = 2
        else:
            fig_col = rank
        fig_row = int(np.ceil(rank/3))
        fig, axes = plt.subplots(fig_row, fig_col, figsize=(15, fig_row*5), sharey=True)
        fig.subplots_adjust(hspace=0.5)
        fig.suptitle(title)

        j = 0
        data_cols = scoredDF.filter(regex='s_').columns
        for ax, column in zip_longest(axes.flat,data_cols,fillvalue=None):
            # turn of the subplot axes if no data exists (zip_longest)
            if column is None: 
                ax.set_axis_off()
                continue
            # j used for component number
            j += 1
            # subplot configuration
            xlabel = "Personality Factors"
            ylabel = "Personality Level (z-value)"
            if not sub:
                title = "Component {}".format(j)
            else:
                title = self.buildString(sub[j-1])
            # plot using seaborn
            sns.barplot(x="layer", y=column, data=scoredDF,ax=ax)
            ax.set(title=title, xlabel=xlabel,ylabel=ylabel)
            ax.set_ylim(ymin,ymax)
        
        # plt.tight_layout()
        if output:
            fpath = os.path.join(output,"personality_activity-components_{}-factor_{}-rank{}_{}nodes.png".format(j,factor,rank,nodes))
            plt.savefig(fpath,dpi=600)
            print("Saved \'{}\'".format(fpath))
        if display:    
            plt.show(block=False)

    def detectCommunities(self,commDF,ffmsDF,rank,nodes,output=None,plot=False,display=False,verbose=False):
        if commDF.empty or ffmsDF.empty:
            print("The dataframes must not be empty...")
            return
       
        if verbose:
            print(commDF.head(10))
            print(ffmsDF.head(10))

        aDF = commDF.filter(regex='A_')
        bDF = commDF.filter(regex='B_')
        cDF = ffmsDF.filter(regex='C_')
        
        # rank of tensor decomp is the number of columns/comonents
        if rank <= 0:
            rank = len(cDF.columns)-1
        
        # create a score df for A (saDF) and B (sbDF)
        saDF = ffmsDF[['layer']].copy()
        sbDF = ffmsDF[['layer']].copy()
        # here we are scoring 
        #   scoreA  = c_k * sum(a_ik), scoreB = c_k * sum(b_jk)
        for column in cDF.columns:
            
            component = column.split('_')[1]
            scol = "s_{}".format(component)
            sumA = aDF.filter(regex=component).sum()
            sumB = bDF.filter(regex=component).sum()
            saDF[scol] = cDF[column] * sumA['A_{}'.format(component)]
            sbDF[scol] = cDF[column] * sumB['B_{}'.format(component)]

        if verbose:
            print(saDF)
            print(sbDF)

        if plot or output:
            self.communityPlot(saDF,rank,nodes,"A",output=output,display=display)
            self.communityPlot(sbDF,rank,nodes,"B",output=output,display=display)

        return saDF,sbDF
            


    def run(self):
        """MultilayerAnalsis.run()
        Main function that runs the postprocessing
        """

        print("\n----------------------------------------")
        print("Starting \'{}\'".format(self.__class__.__name__))
        print("----------------------------------------\n")

        plt.close('all')

        print("Collecting the data...")
        analysis_paths = []
        graph_paths = []
        factors = ['community','personality']
        ffmTypes = ['OPN','CSN','EXT','AGR','NEU']
        ffmDFs = []
        rank = -1
        nodes = -1
        for file in os.listdir(self.csv_path):
            file_path = os.path.join(self.csv_path, file)
            # load the CPD factors
            if file.endswith(".csv"):
                # analysis_paths.append(os.path.join(self.csv_path, file))
                if 'community' in file:
                    commDF = pd.read_csv(file_path)
                    nodes = commDF['id'].count()
                if 'personality' in file:
                    ffmsDF = pd.read_csv(file_path)
                    rank = len(ffmsDF.columns)-1
                if 'lambda':
                    lmdaDF = pd.read_csv(file_path)
            else:
                # load the vertices/nodes
                if 'graph' in file:
                    for gfile in os.listdir(file_path):
                        gfile_path = os.path.join(file_path, gfile)
                        if gfile.endswith(".csv") and 'vert' in gfile:
                            ffmDFs.append(pd.read_csv(gfile_path))
        
        if self.graph_path:
            for gfile in os.listdir(self.graph_path):
                gfile_path = os.path.join(self.graph_path, gfile)
                if gfile.endswith(".csv") and 'vert' in gfile:
                    ffmDFs.append(pd.read_csv(gfile_path))
        
        # merge all FFM scores
        mergeCols = [x for x in ffmDFs[0].columns if not x.endswith('_Z')]
        nodeDF = reduce(lambda left,right: pd.merge(left,right,on=mergeCols), ffmDFs)
                # if any(sub in file for sub in factors):
                #     detect_paths.append(file_path)
        
        print("Detecting the communities...")
        aScored,bScored = self.detectCommunities(commDF,ffmsDF,rank,nodes,output=self.csv_path,display=True,)#output=self.csv_path)

        # print("The best scored component of A is: \n{}".format(aScored))

        # profile = [("OPN",1),("CSN",0),("EXT",1),("AGR",0),("NEU",-1)]
        profile = [("OPN",1),("CSN",1),("EXT",1),("AGR",0),("NEU",-1)]
        print("\nProfile for matching:\n>> {} <<\n".format(profile))
        print("Picking the best tensor components given the profile...")
        compA,aDF = self.highlightComponents(aScored,mask=profile)
        compB,bDF = self.highlightComponents(bScored,mask=profile)

        bestA = "A_"+compA
        bestB = "B_"+compB
        
        compA_d = re.findall('\d+',compA)[0]
        compB_d = re.findall('\d+',compB)[0]
        
        # let's save the top A and B factor plots
        abDF = aDF.merge(bDF,on="layer")
        title = "Best A/B Components for Profile Matching"
        subtitles = [("Component {}",compA_d),("Component {}",compB_d)]
        self.communityPlot(abDF,2,nodes,bestA,title=title,sub=subtitles,output=self.csv_path,display=True) # rank=1 triggers single plot
        

        topn = 5
        print("Select the top {} nodes from the selected component...".format(topn))
        topA = self.getTopN(bestA,commDF,N=topn)#,values=True) # return numpy.ndarray
        topB = self.getTopN(bestB,commDF,N=topn)#,values=True) # return numpy.ndarray


        print("Get the top {} nodes from the original dataset...".format(topn))
        uuidA,bestADF = self.getNodes(nodeDF,topA)
        uuidB,bestBDF = self.getNodes(nodeDF,topB)

        # reset ID and order
        bestADF = bestADF.set_index('id').loc[uuidA]
        bestBDF = bestBDF.set_index('id').loc[uuidB]
        print("\n------------------------------------------------")
        print("Analysis for rank-{} decomposition of {} nodes:".format(rank,nodes))
        print("\nOur profile:")
        print(profile)
        print("The best component of A is: Component {}".format(compA_d))
        print("The best component of B is: Component {}".format(compB_d))
        print("The best A users")
        print(bestADF.head(topn))
        print("The best B users")
        print(bestBDF.head(topn))
        print("------------------------------------------------\n")

        # display the plots
        plt.show()

     

if __name__ == "__main__":
    prg_desc = 'Postprocessed personality project data. For more'
    prg_desc += 'information please refer to the git repository: '
    prg_desc += '{}'.format(GIT_REPO)

    parser = argparse.ArgumentParser(description=prg_desc)

    parser.add_argument('-i','--input',metavar='INPUT_PATH',
                        dest='csv_path',help='Path to data that needs processed.')
    parser.add_argument('-g','--graph',metavar='GRAPH_PATH',
                        dest='graph_path',help='Path to supplemental graph data.')
    parser.add_argument('-y','--pretty',action='store_true',default=False,
                        dest='pretty',help='Enables pretty output of all files.')
    parser.add_argument('-v','--verbose',action='store_true',default=False,
                        dest='verbose',help='Verbose output.')

    args = parser.parse_args()

    analyze = MultilayerAnalysis(**vars(args))

    analyze.run()

          



