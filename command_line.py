import json
import namenode
import main
import dnode
import hadoop_mapreduce
import subprocess
alive = 1
glob_config={}

def hadoop_config(command):
    global glob_config
    try:
        if(len(command) < 2):
            print('No path specified')
            return None
        else:
            config_path = command[1]
            logs = open(command[1])
            glob_config = json.load(logs)
            main.create_datanode(
                glob_config['num_datanodes'], glob_config['datanode_size'], glob_config['path_to_datanodes'])
            main.create_namenode(glob_config['path_to_namenodes'],glob_config['fs_path'],glob_config['namenode_checkpoints'])
            main.create_datanode_logfiles(glob_config['datanode_log_path'],glob_config['num_datanodes'])
            main.create_namenode_logfiles(glob_config['namenode_log_path'],glob_config['num_datanodes'])
            main.create_datanode_tracker(glob_config['path_to_namenodes'],glob_config['num_datanodes'],glob_config['path_to_datanodes'],glob_config['datanode_size'])
            subprocess.Popen(["python","heart.py",glob_config['path_to_namenodes'],glob_config['namenode_checkpoints']])
    except Exception as e:
        print(e)
        return None


def put(command):
    try:
        if(len(command) < 2):
            print('No path specified')
            return None
        else:
            var = dnode.initial_split(command[2],int(glob_config["block_size"]), glob_config["datanode_size"], glob_config["num_datanodes"],glob_config["path_to_datanodes"],glob_config["path_to_namenodes"],glob_config['datanode_log_path'],glob_config['namenode_log_path'],glob_config['fs_path'],command[1])
            if(var==-1):
                pass
            else:
                dnode.replicate_files(command[2],int(glob_config["block_size"]), glob_config["datanode_size"], glob_config["num_datanodes"],glob_config["path_to_datanodes"],glob_config["path_to_namenodes"],glob_config["replication_factor"],glob_config['datanode_log_path'],glob_config['namenode_log_path'],glob_config['fs_path'],command[1])
    except Exception as e:
        print(e)
        return None

def mkdir(command):
    try:
        if(len(command) < 2):
            print('No path specified')
            return None
        else:
            namenode.mkdir(glob_config["path_to_namenodes"],glob_config["fs_path"],command[1])
    except Exception as e:
        print(e)
        return None

def cat(command):
    try:
        if(len(command) < 2):
            print('No path specified')
            return None
        else:
            namenode.cat(glob_config["path_to_namenodes"],glob_config["path_to_datanodes"],glob_config["fs_path"],command[1],glob_config["namenode_log_path"],glob_config["datanode_log_path"],glob_config["num_datanodes"],0)
    except Exception as e:
        print(e)
        return None

def ls(command):
    try:
        if(len(command) < 2):
            print('No path specified')
            return None
        else:
            namenode.ls(glob_config["path_to_namenodes"],glob_config["fs_path"],command[1])
    except Exception as e:
        print(e)
        return None

def rm(command):
    try:
        if(len(command) < 2):
            print('No path specified')
            return None
        else:
            namenode.rm(glob_config["path_to_namenodes"],glob_config["path_to_datanodes"],glob_config["fs_path"],command[1],glob_config["datanode_log_path"],glob_config["namenode_log_path"],glob_config["num_datanodes"])
    except Exception as e:
        print(e)
        return None


def rmdir(command):
    try:
        if(len(command) < 2):
            print('No path specified')
            return None
        else:
            namenode.rmdir(glob_config["path_to_namenodes"],glob_config["path_to_datanodes"],glob_config["fs_path"],command[1])
    except Exception as e:
        print(e)
        return None

def mapreduce(command):
    try:
        if(len(command) < 2):
            print('No path specified')
            return None
        else:
            hadoop_mapreduce.map_reducer(command[1],command[2],command[4],glob_config["fs_path"],command[3],glob_config["path_to_namenodes"],glob_config["path_to_datanodes"],glob_config["namenode_log_path"],glob_config["datanode_log_path"],glob_config["num_datanodes"])
    except Exception as e:
        print(e)
        return None


print('Entering Hadoop Terminal')
while(alive):
    try:
        command = input('>')
        command = command.split()
        # print(command)
        if(command[0] == 'exit'):
            alive = 0
        else:
            # hadoop config config.js
            if(command[1] == 'config'):
                hadoop_config_out = hadoop_config(command[1:])
                if(hadoop_config_out) == None:
                    pass

            # put operation
            elif(command[1] == 'put'):
                put(command[1:])

            # mkdir operation
            elif(command[1] == 'mkdir'):
                mkdir(command[1:])

            # cat operation
            elif(command[1] == 'cat'):
                cat(command[1:])
            
            # ls operation
            elif(command[1] == 'ls'):
                ls(command[1:])

            # rm operation
            elif(command[1] == 'rm'):
                rm(command[1:])

            # rmdir operation
            elif(command[1] == 'rmdir'):
                rmdir(command[1:])
            
            # hadoop map reduce
            elif(command[1] == 'mapreduce'):
                mapreduce(command[1:])
    except:
        pass
