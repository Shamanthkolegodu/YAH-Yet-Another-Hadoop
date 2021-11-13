import os
# os.mkdir('path_to_datanodes') 
num_of_dnodes=4
data_node_track={}
# creates direcories which act as datanodes 
def datanode_creation(num_of_datanode):
	for data_node_number in range(1,num_of_dnodes+1):
		os.mkdir('path_to_datanodes/'+str(data_node_number)+"_data_node")
		data_node_track[data_node_number]=0
		# f = open(os.path.expanduser(os.path.join(str(data_node_number) + "_data_node.txt")), "a")
# datanode_creation(num_of_dnodes)  

# creates a chunk of file
def split_files(filename, chunksize,datanode_number):
	with open(filename + '.txt', 'rb') as bytefile:
		content = bytearray(os.path.getsize(filename + '.txt'))
		bytefile.readinto(content)
		
		for count, i in enumerate(range(0, len(content), chunksize)):
			with open('path_to_datanodes/'+str(datanode_number)+'_data_node/'+filename + '_' + str(count+1) + '.txt.', 'wb') as fh:
				fh.write(content[i: i + chunksize])
split_files('a',5*1024,1)

print(data_node_track)
def hash_function(filename,chunksize):
	size=os.path.getsize(filename + '.txt')

