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
	hash_val = (file_block_no % num_of_datanodes)
	if(datanode[hash_val+1]>0):
		return hash_val+1
	else:
		j=hash_val+1
		while(datanode[j]==0 and j<num_of_datanodes):
			j=j+1
		if(datanode[j]!=0):
			return j
		else:
			return -1


# creates directories which act as datanodes 
def create_datanodes(num_datanodes,datanode_size):
	for dnode in range(1,num_datanodes+1):
		os.mkdir('Datanodes/'+str(dnode)+"_data_node")
		datanode[dnode]=datanode_size


create_datanodes(num_of_dnodes,4)  

# creates a chunk of file
def split_files(filename,block_size,datanode_size,replication_factor):
	with open(filename, 'rb') as bytefile:
		json_of_files[filename]={}
		content = bytearray(os.path.getsize(filename))
		bytefile.readinto(content)
		file_splits=len(content)/block_size
		available_blocks=0
		for key,value in datanode.items():
			available_blocks+=value
		if(available_blocks<file_splits):
			print('Not enough space to store the file!')
			return
		for block_number, i in enumerate(range(0, len(content), block_size)):
			json_of_files[filename][block_number+1]={}
			hashed_val = hashing(block_number,num_of_dnodes)
			if(hashed_val==-1):
				print('Not space to store the file!')
				return
			dnode_block = datanode_size-datanode[hashed_val]+1
			with open(f'Datanodes/{hashed_val}' + '_data_node/' + str(dnode_block) + '.txt', 'wb') as fh:
				json_of_files[str(filename)][block_number+1][hashed_val]=block_number
				datanode[hashed_val]-=1
				fh.write(content[i: i + block_size])
			fh.close()
		bytefile.close()

		remaining_blocks=0
		for key,value in datanode.items():
			remaining_blocks += value
		can_replicate=min(replication_factor-1,int(remaining_blocks/file_splits))
		if(can_replicate<1):
			print('No space to replicate!')
			return
		for block_number, i in enumerate(range(0, len(content), block_size)):
			hashed_val = hashing(block_number,num_of_dnodes)
			curr_block = json_of_files[str(filename)][block_number+1][hashed_val]
			with open(f'Datanodes/{hashed_val}' + '_data_node/' + str(curr_block) + '.txt', 'rb') as bytefile:
				content = bytearray(os.path.getsize(filename))
				bytefile.readinto(content)
				for count_replicate in range(1,can_replicate+1):
					next_dnode=((hashed_val-1+count_replicate)%datanode_size)+1
					next_dnode_block = datanode_size-datanode[next_dnode]+1
					with open(f'Datanodes/{next_dnode}' + '_data_node/' + str(next_dnode_block) + '.txt', 'wb') as fh:
						json_of_files[str(filename)][block_number+1][next_dnode]=next_dnode_block
						datanode[next_dnode]-=1
						fh.write(content[i: i + block_size])
					fh.close()
			bytefile.close()


split_files('xD.txt',1000 * 500,4,4)
print(json_of_files)
print(datanode)
