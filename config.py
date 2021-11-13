#!/usr/bin/python3

import os
import sys
import json
# os.mkdir('path_to_datanodes') 
num_of_dnodes=4
rep_factor = 2

mapper = {}

def hashing(file_block_no, num_of_datanodes):
    return (file_block_no % num_of_datanodes) + 1

# creates direcories which act as datanodes 
def datanode_creation(num_of_datanode):
	for data_node_number in range(1,num_of_dnodes+1):
		os.mkdir('Datanodes/'+str(data_node_number)+"_data_node")
		mapper[str(data_node_number)+"_data_node"] = {}
#datanode_creation(num_of_dnodes)  

# creates a chunk of file
def split_files(filename, chunksize):
	# try:
	# 	logs = open('Namenodes/sample.json')
	# except Exception as e:
	# 	logs = open('Namenodes/sample.json')
	# mapper = json.load(logs)
 
	with open(filename + '.txt', 'rb') as bytefile:
		content = bytearray(os.path.getsize(filename + '.txt'))
		bytefile.readinto(content)
		for count, i in enumerate(range(0, len(content), chunksize)):
			hashed = hashing(count,num_of_dnodes)
			block_no_in_dn = len(mapper[f'{hashed}_data_node'])
			with open(f'Datanodes/{hashed}' + '_data_node/' + str(block_no_in_dn+1) + '.txt.', 'wb') as fh:
				mapper[f'{hashed}_data_node'][block_no_in_dn+1] = f'{filename}_{count+1}.txt'
				fh.write(content[i: i + chunksize])
		fh.close()
	bytefile.close()
	json.dump(mapper,logs)
	logs.close()
			
split_files('xD',1000 * 1000)
print(mapper)
