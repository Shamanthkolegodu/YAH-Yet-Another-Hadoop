import json
import helper


alive = 1
# functions
# hadoop config /Users/shamanthkm/Desktop/config_sample.json


def hadoop_config(command):
    try:
        if(len(command) < 2):
            print('No path specified')
            return None
        else:
            logs = open(command[1])
            config = json.load(logs)
            helper.create_datanode(
                config['num_datanodes'], config['datanode_size'], config['path_to_datanodes'])
            helper.create_namenode(config['path_to_namenodes'])
    except Exception as e:
        print(e)
        return None


def put(command):
    print(command)


# hadoop config config.js
print('Entering haddop terminal')
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
