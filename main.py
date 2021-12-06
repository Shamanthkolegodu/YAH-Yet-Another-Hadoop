#!/usr/bin/python3
import os
import sys
import json
from datetime import datetime
import namenode


files_json = {}
datanode = {}


def create_datanode(num_datanodes, datanode_size, datanode_path):
    datanode_dict = {}
    for dnode in range(1, num_datanodes+1):
        os.makedirs(datanode_path+str(dnode)+"_data_node")
        datanode[dnode] = datanode_size
    
	
def create_namenode(Namenode_path,fs_path,namenode_checkpoints):
    os.makedirs(Namenode_path)
    os.makedirs(namenode_checkpoints)

    primary_json = {
        fs_path:{}
    }
    with open(Namenode_path+'primary.json', 'w') as primary:
        json.dump(primary_json, primary)
        primary.close()
        
    with open(Namenode_path+'secondary.json', 'w') as secondary:
        json.dump(primary_json, secondary)
        secondary.close()
    
    with open(namenode_checkpoints+'Checkpoints.txt','w') as ch_pt:
        ch_pt.write("!-------CHECKPOINTS INITIATED-------!\n\n\n")
        ch_pt.close()

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

def create_datanode_tracker(namenode_path,num_datanodes,datanode_path,datanode_size):
    datanode_dict = {}
    for dnode in range(1, num_datanodes+1):
        datanode_dict[str(dnode)] = {}
        for i in range(1,datanode_size+1):
            datanode_dict[str(dnode)][str(i)] = 0
        datanode_dict[str(dnode)]["count"] = datanode_size
    with open(namenode_path + "dnode_tracker.json" , "w") as dnode_track:
        json.dump(datanode_dict,dnode_track)
        dnode_track.close()