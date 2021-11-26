#!/usr/bin/python3
import os
import sys
import json
from datetime import datetime
import namenode


files_json = {}
datanode = {}


def create_datanode(num_datanodes, datanode_size, Datanode_path):
    for dnode in range(1, num_datanodes+1):
        os.makedirs(Datanode_path+str(dnode)+"_data_node")
        datanode[dnode] = datanode_size
	
def create_namenode(Namenode_path):
	os.makedirs(Namenode_path)
	primary_json = {}
	with open(Namenode_path+'primary.json', 'w') as primary:
		json.dump(primary_json, primary)

def create_datanode_logfiles(logfile_path,num_datanodes):
    os.mkdir(logfile_path)
    for dnode in range(1, num_datanodes+1):
        with open(f'{logfile_path}{dnode}_datanode_log.txt',"w+") as logfile:
            logfile.write(f"Datanode {dnode} has been created, {datetime.now()}\n")
        logfile.close()

def create_namenode_logfiles(logfile_path,num_datanodes):
    with open(f'{logfile_path}',"w+") as logfile:
        for i in range(1,num_datanodes+1):
            logfile.write(f"Datanode {i} has been created , {datetime.now()}\n")
    logfile.close()