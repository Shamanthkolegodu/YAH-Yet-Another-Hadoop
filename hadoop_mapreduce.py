import namenode
import os
import json
import subprocess

def map_reducer(mapper_absolute_path,reducer_absolute_path,output_absolute_path,fs_path,file_path,namenode_path,datanode_path,namenode_logfile_path,datanode_logfile_path,num_of_datanodes):
    check_path = fs_path + file_path
    name = file_path.split('/')[-1]
    global contents
    with open(os.path.join(namenode_path , 'primary.json')) as tracks:
        contents = json.loads(tracks.read())
        tracks.close()
    if check_path not in contents:
        print("Input File doesnt exist")
    else:
        global content
        global mapper_content
        namenode.cat(namenode_path,datanode_path,fs_path,file_path,namenode_logfile_path,datanode_logfile_path,num_of_datanodes,1)
        var = namenode_path + 'hadoop_' + name
        with open(var,"r") as primary:
            content = primary.read()
            primary.close()
        with open("result.txt","w") as answer:
            answer.writelines(content)
            answer.close()
        os.system('cat result.txt | ' + 'python3 '+mapper_absolute_path+'>mout.txt')
        os.system("cat mout.txt | sort -k 1,1 | python3 "+reducer_absolute_path+' >'+output_absolute_path)
        


