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

def mkdir(namenode_path,fs_path,directory_name):
    arrays = directory_name.split('/')
    path ='/'.join(arrays[:-1])
    if len(path)==0:
        check_path = fs_path + path
    else:
        check_path = fs_path + path + '/'
    directory_path = fs_path + directory_name + '/'
    global content
    with open(os.path.join(namenode_path , 'primary.json')) as primary:
        content = json.loads(primary.read())
        if(check_path not in content):
            print("Directory doesnt exist")
        else:
            content[directory_path] = {}
        primary.close()
    with open(namenode_path+'primary.json', 'w') as primary:
        json.dump(content,primary)


# def cat(namenode_path,datanode_path,fs_path,file_path):
#     check_path = fs_path + file_path
#     global content
#     with open(os.path.join(namenode_path , 'primary.json')) as primary:
#         content = json.loads(primary.read())
#         if(check_path not in content):
#             print("File not found")
#         else:
#             current_dict = content[check_path]
#             n = len(current_dict)
#             for i in range(1,n+1):
                



        
        
        


        






        