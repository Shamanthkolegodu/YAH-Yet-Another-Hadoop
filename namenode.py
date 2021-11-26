#!/usr/bin/python3
import os
import sys
import json
from datetime import datetime
from dnode import datanode

def update_namenode_logfile(logfile_path,dnode,block,operation,num_of_datanodes):
    with open(f'{logfile_path}',"a+") as logfile:
        if(operation=='put'):
            logfile.write(f"Datanode {dnode}'s Block {block} has been occupied, {datetime.now()} \n")
            for i in range(1,num_of_datanodes+1):
                logfile.write(f"Remaining Blocks in datanode {i} : {datanode[i]} , {datetime.now()} \n")
    logfile.close()
        