import namenode
import os
import json
import subprocess

def map_reducer(mapper_absolute_path,reducer_absolute_path,output_absolute_path,fs_path,file_path,namenode_path,datanode_path,namenode_logfile_path,datanode_logfile_path,num_of_datanodes):
    check_path = fs_path + file_path
    global contents
    with open(os.path.join(namenode_path , 'primary.json')) as tracks:
        contents = json.loads(tracks.read())
        tracks.close()
    if check_path not in contents:
        print("Input File doesnt exist")
    else:
        variable = namenode.cat(namenode_path,datanode_path,fs_path,file_path,namenode_logfile_path,datanode_logfile_path,num_of_datanodes,1)
        with open("full_data.txt","w") as primary:
            primary.writelines(variable)
            primary.close()
        os.system('cat full_data.txt |'+ ' python3 '+mapper_absolute_path+' >output.txt')
        


