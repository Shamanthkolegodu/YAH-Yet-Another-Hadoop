#!/usr/bin/python3
import os
import sys
import json
from datetime import datetime
from main import datanode
import namenode
files_json = {}



def update_datanode_logs(dnode,block,dnode_logfile_path,operation):
    with open(f'{dnode_logfile_path}{dnode}_datanode_log.txt',"a+") as logfile:
        if(operation=='put'):
            logfile.write(f"Block {block} has been occupied, {datetime.now()} \n")
        if(operation=='rm'):
            logfile.write(f"Block {block} has been removed, {datetime.now()} \n")
        if(operation=="cat"):
            logfile.write(f"Block {block} contents have been read and displayed, {datetime.now()} \n")


def hashing(file_block_no,num_of_datanodes,namenode_path,datanode_size):
    global track_reader
    with open(os.path.join(namenode_path , 'dnode_tracker.json')) as primary:
        track_reader = json.loads(primary.read())
        primary.close()
    val = file_block_no % num_of_datanodes
    hash_val = val + 1
    if(track_reader[str(hash_val)]["count"]>0):
        dnode_val = hash_val
        dnode_block = datanode_size - track_reader[str(dnode_val)]["count"] + 1
        if(track_reader[str(dnode_val)][str(dnode_block)]==0):
            return (dnode_val,dnode_block)
        else:
            for i in range(1,datanode_size+1):
                if(track_reader[str(dnode_val)][str(i)]==0):
                    dnode_block = i
                    break
            return (dnode_val,dnode_block)
    elif(track_reader[str(hash_val)]["count"]==0):
        start = 1
        dnode_val = None
        dnode_block = None
        for i in range(1,num_of_datanodes+1):
            if(track_reader[str(i)]["count"]>0):
                dnode_val = i
                break
        for i in range(1,datanode_size+1):
            if(track_reader[str(dnode_val)][str(i)]==0):
                dnode_block = i
                break
        return (dnode_val,dnode_block)
    else:
        return (-1,-1)


def replicaiton_hashing(file_block,num_of_datanodes,namenode_path,datanode_size,fs_path,file_path,cur_node,cur_block):
    global track_reader
    global primary_tracker
    with open(os.path.join(namenode_path , 'dnode_tracker.json')) as primary:
        track_reader = json.loads(primary.read())
        primary.close()
    with open(os.path.join(namenode_path , 'primary.json')) as primary:
        primary_tracker = json.loads(primary.read())
        primary.close()
    if(cur_node==num_of_datanodes):
        next_node = 1
    else:
        next_node = cur_node + 1
    if(track_reader[str(next_node)]["count"]>0):
        dnode_val = next_node
        dnode_block = datanode_size - track_reader[str(dnode_val)]["count"] + 1
        if(track_reader[str(dnode_val)][str(dnode_block)]==0):
            if(cur_node==dnode_val):
                return (-2,-2)
            return (dnode_val,dnode_block)
        else:
            for i in range(1,datanode_size+1):
                if(track_reader[str(dnode_val)][str(i)]==0):
                    dnode_block = i
                    break
            if(cur_node==dnode_val):
                return (-2,-2)
            return (dnode_val,dnode_block)
    else:
        start = 1
        dnode_val = None
        dnode_block = None
        for i in range(start,num_of_datanodes+1):
            if(track_reader[str(i)]["count"]>0):
                dnode_val = i
                break
        for i in range(start,datanode_size+1):
            if(track_reader[str(dnode_val)][str(i)]==0):
                dnode_block = i
                break
        if(cur_node==dnode_val):
            return (-2,-2)
        return (dnode_val,dnode_block)

    
        
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
        global dnode_tracks
        with open(filename, 'rb') as bytefile:
            ext = filename.split('.')[1]
            block_size = block_size * 1024 * 1024
            files_json[fs_path+file_path]={}
            content = bytearray(os.path.getsize(filename))
            bytefile.readinto(content)
            file_splits=len(content)/block_size
            available_blocks=0
            with open(os.path.join(Namenode_path , 'dnode_tracker.json')) as d_tracker:
                dnode_tracks = json.loads(d_tracker.read())
                for i in range(1,len(dnode_tracks)+1):
                    available_blocks += dnode_tracks[str(i)]["count"]
                d_tracker.close()

            if(available_blocks<file_splits):
                print('Less blocks available')
                return
            for file_block, i in enumerate(range(0, len(content), block_size)):
                files_json[fs_path+file_path][file_block+1] = {}
                dnode,block = hashing(file_block,num_datanodes,Namenode_path,datanode_size)
                if(dnode == -1):
                    print('No space to insert file block')
                    return
                with open(f'{path_datanode}{dnode}'+'_data_node/' + str(block) + '.'+ext, 'wb') as fh:
                    files_json[fs_path+file_path][file_block+1][dnode] = block
                    dnode_tracks[str(dnode)][str(block)] = 1
                    dnode_tracks[str(dnode)]["count"] -= 1
                    with open(Namenode_path+'dnode_tracker.json', 'w') as secondary:
                        json.dump(dnode_tracks,secondary)
                        secondary.close()
                    fh.write(content[i: i + block_size])
                    update_datanode_logs(dnode,block,logfile_path,'put')
                    namenode.update_namenode_logfile(namenode_logfile_path,dnode,block,'put',num_datanodes,Namenode_path)
                fh.close()
            bytefile.close()
        global content_to_write
        with open(os.path.join(Namenode_path , 'primary.json')) as primary:
            content_to_write = json.loads(primary.read())
            content_to_write[fs_path+file_path] = files_json[fs_path+file_path]
            primary.close()
        with open(Namenode_path+'primary.json', 'w') as secondary:
            json.dump(content_to_write,secondary)
            secondary.close()
        with open(Namenode_path+'dnode_tracker.json', 'w') as ternary:
            json.dump(dnode_tracks,ternary)
            ternary.close()


def replicate_files(filename,block_size,datanode_size,num_datanodes,path_datanode,Namenode_path,replication_factor,logfile_path,namenode_logfile_path,fs_path,file_path):
    remaining_blocks = 0
    ext = filename.split('.')[1]
    block_size = block_size * 1024 * 1024
    global dnode_tracks
    with open(os.path.join(Namenode_path , 'dnode_tracker.json')) as d_tracker:
        dnode_tracks = json.loads(d_tracker.read())
        for i in range(1,len(dnode_tracks)+1):
            remaining_blocks += dnode_tracks[str(i)]["count"]
        d_tracker.close()
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
        dictis = files_json[fs_path + file_path][file_block+1]
        cur_node = None
        cur_block = None
        for key,value in dictis.items():
            cur_node = key
            cur_block = value
            break
        with open(f'{path_datanode}/{cur_node}' + '_data_node/' + str(cur_block) + '.'+ext, 'rb') as bytefile:
            for file_block_replica in range(1,can_replicate+1):
                dnode,block = replicaiton_hashing(file_block,num_datanodes,Namenode_path,datanode_size,fs_path,file_path,cur_node,cur_block)
                if(dnode==-2):
                    print(f'cannot replicate file block {file_block+1} since the original file block is also on the same data node')
                else:
                    with open(f'{path_datanode}/{dnode}' + '_data_node/' + str(block) + '.'+ext, 'wb') as fh:
                        files_json[fs_path+file_path][file_block+1][dnode] = block
                        dnode_tracks[str(dnode)][str(block)] = 1
                        dnode_tracks[str(dnode)]["count"] -= 1
                        with open(Namenode_path+'dnode_tracker.json', 'w') as secondary:
                            json.dump(dnode_tracks,secondary)
                            secondary.close()
                        fh.write(content[i: i + block_size])
                        update_datanode_logs(dnode,block,logfile_path,'put')
                        namenode.update_namenode_logfile(namenode_logfile_path,dnode,block,'put',num_datanodes,Namenode_path)
                    fh.close()
        bytefile.close()    
    with open(os.path.join(Namenode_path , 'primary.json')) as primary:
        content_to_write = json.loads(primary.read())
        content_to_write[fs_path+file_path] = files_json[fs_path+file_path]
        primary.close()
    with open(Namenode_path+'primary.json', 'w') as primary:
        json.dump(content_to_write,primary)
        primary.close()
    with open(Namenode_path+'dnode_tracker.json', 'w') as primary:
        json.dump(dnode_tracks,primary)
        primary.close()