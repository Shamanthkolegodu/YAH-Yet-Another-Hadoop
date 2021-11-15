#!/usr/bin/python3

import os
import sys
import json
# os.mkdir('path_to_datanodes') 
num_of_dnodes=4
replication_facor = 2
'''
files_json = {
	"path/a.txt":{
		1:{
			1:1,
			2:2
		},
		2:{
			2:1,
			3:1
		}
	},
	"path/b.txt":{
		1:{
			1:1,
			2:2
		},
		2:{
			2:1,
			3:1
		}
	}
}
'''
files_json={}

datanode={}


def hashing(file_block_no, num_of_datanodes):
	# return (file_block_no % num_of_datanodes)+1
hashed_value=file_block_no % num_of_datanodes
	if(datanode[hashed_value+1]>0):
		return hashed_value+1
	else:
		j=hashed_value+1
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
def split_files(filename, block_size,datanode_size,replication_facor):
	# try:
	# 	logs = open('Namenodes/sample.json')
	# except Exception as e:
	# 	logs = open('Namenodes/sample.json')
	# files_json = json.load(logs)
	# logs=open('Namenodes/sample.json','w')
	with open(filename, 'rb') as bytefile:
		files_json[filename]={}
		content = bytearray(os.path.getsize(filename))
		bytefile.readinto(content)
		file_splits=len(content)/block_size
		available_blocks=0
		for key,value in datanode.items():
			available_blocks+=value
		if(available_blocks<file_splits):
		if(available_blocks<file_splits):
			print('Less blocks available')
			return
		for file_block, i in enumerate(range(0, len(content), block_size)):
			files_json[filename][file_block+1]={}
			hash_value = hashing(file_block,num_of_dnodes)
			if(hash_value==-1):
				print('No space')
				return
			dnode_block = datanode_size-datanode[hash_value]+1
			with open(f'Datanodes/{hash_value}' + '_data_node/' + str(dnode_block) + '.txt', 'wb') as fh:
				files_json[str(filename)][file_block+1][hash_value]=dnode_block
				datanode[hash_value]-=1
				fh.write(content[i: i + block_size])
			fh.close()
		bytefile.close()

		remaining_blocks=0
		for key,value in datanode.items():
			remaining_blocks+=value
		can_replicate=min(replication_facor-1,int(remaining_blocks/file_splits))
		if(can_replicate<1):
			print('No space to replicate')
			return
		for file_block, i in enumerate(range(0, len(content), block_size)):
			hash_value = hashing(file_block,num_of_dnodes)
			current_block=files_json[str(filename)][file_block+1][hash_value]
			with open(f'Datanodes/{hash_value}' + '_data_node/' + str(current_block) + '.txt', 'rb') as bytefile:
				content = bytearray(os.path.getsize(filename))
				bytefile.readinto(content)
				for file_block_replica in range(1,can_replicate+1):
					next_dnode=((hash_value-1+file_block_replica)%datanode_size)+1
					next_dnode_block = datanode_size-datanode[next_dnode]+1
					with open(f'Datanodes/{next_dnode}' + '_data_node/' + str(next_dnode_block) + '.txt', 'wb') as fh:
						files_json[str(filename)][file_block+1][next_dnode]=next_dnode_block
						datanode[next_dnode]-=1
						fh.write(content[i: i + block_size])
					fh.close()
			bytefile.close()


split_files('xD.txt',1000 * 500,4,4)
print(files_json)
print(datanode)
