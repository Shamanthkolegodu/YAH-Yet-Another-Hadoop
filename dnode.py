#!/usr/bin/python3
import os
import sys
import json
from datetime import datetime
from main import datanode
import namenode
files_json = {}



def update_logs(dnode,block,dnode_logfile_path,operation):
    with open(f'{dnode_logfile_path}{dnode}_datanode_log.txt',"a+") as logfile:
        if(operation=='put'):
            logfile.write(f"Block {block} has been occupied, {datetime.now()} \n")


def hashing(file_block_no, num_of_datanodes):
    hashed_value = file_block_no % num_of_datanodes
    new_hashed_val = hashed_value + 1
    if(datanode[new_hashed_val] > 0):
        return new_hashed_val
    else:
        if(new_hashed_val==num_of_datanodes):
            j = 1
            while(datanode[j]==0 and j<num_of_datanodes):
                j = j+1
            if(datanode[j]!=0):
                return j
            else:
                return -1
        else:
            j = new_hashed_val + 1
        while(datanode[j] == 0 and j < num_of_datanodes):
            j = j+1
        if(datanode[j]!=0):
            return j
        elif(j+1==num_of_datanodes and datanode[j]!=0):
            return j+1
        elif(j+1==num_of_datanodes and datanode[j]==0):
            j = 1
            while(datanode[j]==0 and j<num_of_datanodes):
                j = j + 1
            if(datanode[j]!=0):
                return j
        else:
            return -1

def initial_split(filename,block_size,datanode_size,num_datanodes,path_datanode,Namenode_path,logfile_path,namenode_logfile_path,fs_path,file_path):
        splits = file_path.split('/')
        path ='/'.join(splits[:-1])
        if len(path)==0:
            check_path = fs_path + path
        else:
            check_path = fs_path + path + '/'
        with open(os.path.join(Namenode_path , 'primary.json')) as primary:
            intial_read = json.loads(primary.read())
            if check_path not in intial_read:
                print("Directory doesnt exist")
                return -1
            elif fs_path+file_path in intial_read:
                print("File already exist")
                return -1
        primary.close()
        with open(filename, 'rb') as bytefile:
            ext = filename.split('.')[1]
            block_size = block_size * 1024 * 1024
            files_json[fs_path+file_path]={}
            content = bytearray(os.path.getsize(filename))
            bytefile.readinto(content)
            file_splits=len(content)/block_size
            available_blocks=0
            for key,value in datanode.items():
                available_blocks+=value
            if(available_blocks<file_splits):
                print('Less blocks available')
                return
            for file_block, i in enumerate(range(0, len(content), block_size)):
                files_json[fs_path+file_path][file_block+1] = {}
                hash_value = hashing(file_block, num_datanodes)
                if(hash_value == -1):
                    print('No space to insert file block')
                    return
                dnode_block = datanode_size-datanode[hash_value]+1
                with open(f'{path_datanode}{hash_value}'+'_data_node/' + str(dnode_block) + '.'+ext, 'wb') as fh:
                    files_json[str(fs_path+file_path)][file_block +
                                          1][hash_value] = dnode_block
                    datanode[hash_value] -= 1
                    fh.write(content[i: i + block_size])
                    update_logs(hash_value,dnode_block,logfile_path,'put')
                    namenode.update_namenode_logfile(namenode_logfile_path,hash_value,dnode_block,'put',num_datanodes)
                fh.close()
            bytefile.close()
        global content_to_write
        with open(os.path.join(Namenode_path , 'primary.json')) as primary:
            content_to_write = json.loads(primary.read())
            content_to_write[fs_path + file_path] = files_json
            primary.close()
        with open(Namenode_path+'primary.json', 'w') as primary:
            json.dump(content_to_write,primary)
            primary.close()


def replicate_files(filename,block_size,datanode_size,num_datanodes,path_datanode,Namenode_path,replication_factor,logfile_path,namenode_logfile_path,fs_path,file_path):
    remaining_blocks = 0
    ext = filename.split('.')[1]
    block_size = block_size * 1024 * 1024
    for key,value in datanode.items():
        remaining_blocks += value
    with open(filename, 'rb') as bytefile:
        content = bytearray(os.path.getsize(filename))
        bytefile.readinto(content)
        file_splits=len(content)/block_size
        can_replicate = min(replication_factor-1,int(remaining_blocks/file_splits))
        if(can_replicate<1):
            print('No Space to replicate')
            return
    bytefile.close()
    global content_to_write
    for file_block, i in enumerate(range(0, len(content), block_size)):
        hash_value = hashing(file_block,num_datanodes)
        current_block=files_json[str(fs_path+file_path)][file_block+1][hash_value]
        with open(f'{path_datanode}/{hash_value}' + '_data_node/' + str(current_block) + '.'+ext, 'rb') as bytefile:
            for file_block_replica in range(1,can_replicate+1):
                next_dnode=((hash_value-1+file_block_replica)%num_datanodes)+1
                next_dnode_block = datanode_size-datanode[next_dnode]+1
                with open(f'{path_datanode}/{next_dnode}' + '_data_node/' + str(next_dnode_block) + '.'+ext, 'wb') as fh:
                    files_json[str(fs_path+file_path)][file_block+1][next_dnode]=next_dnode_block
                    datanode[next_dnode]-=1
                    fh.write(content[i: i + block_size])
                    update_logs(next_dnode,next_dnode_block,logfile_path,'put')
                    namenode.update_namenode_logfile(namenode_logfile_path,next_dnode,next_dnode_block,'put',num_datanodes)
                fh.close()
        bytefile.close()    
    with open(os.path.join(Namenode_path , 'primary.json')) as primary:
        content_to_write = json.loads(primary.read())
        content_to_write[fs_path+file_path] = files_json[fs_path+file_path]
        primary.close()
    with open(Namenode_path+'primary.json', 'w') as primary:
        json.dump(content_to_write,primary)
        primary.close()