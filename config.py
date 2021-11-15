#!/usr/bin/python3

import os
import sys
import json
# os.mkdir('path_to_datanodes') 
num_of_dnodes=4
rep_factor = 2
json_of_files={}

datanode={}

def hashing(file_block_no, num_of_datanodes):
	# return (file_block_no % num_of_datanodes)+1
	out=file_block_no % num_of_datanodes
	if(datanode[out+1]>0):
		return out+1
	else:
		j=out+1
		while(datanode[j]==0 and j<num_of_datanodes):
			j=j+1
		if(datanode[j]!=0):
			return j
		else:
			return -1
# creates direcories which act as datanodes 
def datanode_creation(num_of_datanode,datanode_size):
	for data_node_number in range(1,num_of_dnodes+1):
		os.mkdir('Datanodes/'+str(data_node_number)+"_data_node")
		datanode[data_node_number]=datanode_size
datanode_creation(num_of_dnodes,4)  

# creates a chunk of file
def split_files(filename, chunksize,datanode_size,rep_factor):
	# try:
	# 	logs = open('Namenodes/sample.json')
	# except Exception as e:
	# 	logs = open('Namenodes/sample.json')
	# json_of_files = json.load(logs)
	# logs=open('Namenodes/sample.json','w')
	with open(filename, 'rb') as bytefile:
		json_of_files[filename]={}
		content = bytearray(os.path.getsize(filename))
		bytefile.readinto(content)
		num_of_blocks_in_input=len(content)/chunksize
		available_blocks=0
		for key,value in datanode.items():
			available_blocks+=value
		if(available_blocks<num_of_blocks_in_input):
			print('Less blocks available')
			return
		for count, i in enumerate(range(0, len(content), chunksize)):
			json_of_files[filename][count+1]={}
			hashed = hashing(count,num_of_dnodes)
			if(hashed==-1):
				print('No space')
				return
			block_no_in_dn = datanode_size-datanode[hashed]+1
			with open(f'Datanodes/{hashed}' + '_data_node/' + str(block_no_in_dn) + '.txt', 'wb') as fh:
				json_of_files[str(filename)][count+1][hashed]=block_no_in_dn
				datanode[hashed]-=1
				fh.write(content[i: i + chunksize])
			fh.close()
		bytefile.close()

		remaining_blocks=0
		for key,value in datanode.items():
			remaining_blocks+=value
		replica_fac=min(rep_factor-1,int(remaining_blocks/num_of_blocks_in_input))
		if(replica_fac<1):
			print('No space to replicate')
			return
		for count, i in enumerate(range(0, len(content), chunksize)):
			hashed = hashing(count,num_of_dnodes)
			input_block=json_of_files[str(filename)][count+1][hashed]
			with open(f'Datanodes/{hashed}' + '_data_node/' + str(input_block) + '.txt', 'rb') as bytefile:
				content = bytearray(os.path.getsize(filename))
				bytefile.readinto(content)
				for replica_count in range(1,replica_fac+1):
					hashed_replica=((hashed-1+replica_count)%datanode_size)+1
					block_no_in_dn_replica = datanode_size-datanode[hashed_replica]+1
					with open(f'Datanodes/{hashed_replica}' + '_data_node/' + str(block_no_in_dn_replica) + '.txt', 'wb') as fh:
						json_of_files[str(filename)][count+1][hashed_replica]=block_no_in_dn_replica
						datanode[hashed_replica]-=1
						fh.write(content[i: i + chunksize])
					fh.close()
			bytefile.close()


split_files('xD.txt',1000 * 500,4,4)
print(json_of_files)
print(datanode)
