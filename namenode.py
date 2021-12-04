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
        elif(directory_path in content):
            print("Directory already exists")
        else:
            content[directory_path] = {}
        primary.close()
    with open(namenode_path+'primary.json', 'w') as primary:
        json.dump(content,primary)
        primary.close()


def cat(namenode_path,datanode_path,fs_path,file_path):
    extensions = file_path.split('.')[-1]
    check_path = fs_path + file_path
    global content_cat
    with open(os.path.join(namenode_path , 'primary.json')) as primary:
        content_cat = json.loads(primary.read())
        if(check_path not in content_cat):
            print("File not found")
        else:
            current_dict = content_cat[check_path]
            for i in range(1,len(current_dict)+1):
                empty_list = []
                for key,value in current_dict[str(i)].items():
                    empty_list.append((key,value))
                    data_node = empty_list[0][0]
                    data_node_block = empty_list[0][1]
                    final_path = datanode_path + str(data_node) + '_data_node/' + str(data_node_block) + '.' + extensions
                    with open(final_path,"r") as file_read:
                        content_to_display = file_read.read()
                        print(content_to_display,end="")
                        file_read.close()
            print("\n")

def ls(namenode_path,fs_path,directory_path):
    check_path = fs_path + directory_path + '/'
    global content_ls
    with open(os.path.join(namenode_path , 'primary.json')) as primary:
        content_ls = json.loads(primary.read())
        if check_path not in content_ls:
            print("directory doesnt exist")
        else:
            check_path_splits = check_path.split('/')
            if(check_path_splits[-1] == ''):
                check_path_splits = check_path_splits[:-1]
            n = len(check_path_splits)
            result = []
            for key,value in content_ls.items():
                answer = key.split('/')
                if(answer[-1] == ''):
                    answer = answer[:-1]
                if(answer[:n]==check_path_splits and len(answer)>n):
                    result.append(answer[n])
            if(len(result)==0):
                print("nothing exists in this directory")
            else:
                for i in result:
                    print(i)


# def rm(namenode_path,datanode_path,fs_path,file_path):
#     check_path = fs_path + file_path
            
    



                




        
        
        


        






        