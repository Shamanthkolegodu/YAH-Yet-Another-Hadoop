import json
import helper


alive = 1
# functions
# hadoop config /Users/shamanthkm/Desktop/config_sample.json
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
            helper.create_datanode(
                glob_config['num_datanodes'], glob_config['datanode_size'], glob_config['path_to_datanodes'])
            helper.create_namenode(glob_config['path_to_namenodes'])
            helper.create_logfiles(glob_config['datanode_log_path'],glob_config['num_datanodes'])
    except Exception as e:
        print(e)
        return None


def put(command):
    try:
        if(len(command) < 2):
            print('No path specified')
            return None
        else:
            helper.initial_split(command[1],int(glob_config["block_size"]), glob_config["datanode_size"], glob_config["num_datanodes"],glob_config["path_to_datanodes"],glob_config["path_to_namenodes"],glob_config['datanode_log_path'])
            helper.replicate_files(command[1],int(glob_config["block_size"]), glob_config["datanode_size"], glob_config["num_datanodes"],glob_config["path_to_datanodes"],glob_config["path_to_namenodes"],glob_config["replication_factor"],glob_config['datanode_log_path'])
    except Exception as e:
        print(e)
        return None


# hadoop config config.js
print('Entering hadoop terminal')
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

            # hadoop config config.js
            elif(command[1] == 'put'):
                put(command[1:])
    except:
        pass
