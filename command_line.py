import json
import namenode
import main
import dnode

alive = 1
glob_config={}

def hadoop_config(command):
    global glob_config
    try:
        if(len(command) < 2):
            print('No path specified')
            return None
        else:
            logs = open(command[1])
            glob_config = json.load(logs)
            main.create_datanode(
                glob_config['num_datanodes'], glob_config['datanode_size'], glob_config['path_to_datanodes'])
            main.create_namenode(glob_config['path_to_namenodes'],glob_config['fs_path'])
            main.create_datanode_logfiles(glob_config['datanode_log_path'],glob_config['num_datanodes'])
            main.create_namenode_logfiles(glob_config['namenode_log_path'],glob_config['num_datanodes'])
            main.create_datanode_tracker(glob_config['path_to_namenodes'],glob_config['num_datanodes'],glob_config['path_to_datanodes'],glob_config['datanode_size'])
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
            namenode.cat(glob_config["path_to_namenodes"],glob_config["path_to_datanodes"],glob_config["fs_path"],command[1])
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
            namenode.rm(glob_config["path_to_namenodes"],glob_config["path_to_datanodes"],glob_config["fs_path"],command[1])
    except Exception as e:
        print(e)
        return None


print('Entering Hadoop Terminal')
while(alive):
    try:
        command = input('>')
        command = command.split()
        # print(command)
        if(command[0] == '0'):
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
    except:
        pass
