# YAH – Yet Another Hadoop
YAH is a mini-HDFS setup on your system, complete with the architectural structure consisting of Data Nodes and Name Nodes and replication of data across nodes.

With YAH, you will possess finer control over your data blocks and their replication, and allow you to create a distributed file system as per your needs. YAH also allows running Hadoop like jobs to break down computationally heavy tasks into smaller distributed tasks.

## Setting up the Distributed File System
A single config.json will be provided to you which will contain the configuration details of the DFS. Attached below is the schema for the config file. You may add more attributes to the ones already listed. However note that the listed attributes are mandatory and need to be in every config file.

```
{
    "block_size": int, indicates block size to split files,
    "path_to_datanodes": str, absolute path to create Data Nodes,
    "path_to_namenodes": str, absolute path to create Name Node and secondary Name Node,
    "replication_factor": int, replication factor of each block of a file,
    "num_datanodes": int, number of Data Nodes to create,
    "datanode_size": int, number of blocks a single Data Node may contains,
    "sync_period": int, interval in seconds for the secondary Name Node to sync with the primary Name Node,
    "datanode_log_path": str, absolute path to a directory create a log file to store information about Data Nodes,
    "namenode_log_path": str, absolute path to create a log file to store information about Data Nodes,
    "namenode_checkpoints": str, abolute path to a directory to store Name Node checkpoints,
    "fs_path": str, absolute path to file system where a user may create files,
    "dfs_setup_config": str, absolute path to DFS setup information
}
```

The setup process is expected to read in this config file and create the DFS based on the configuration provided. If no such configuration is provided, it must resort to using a default configuration (which is up to you). For each unique config.json provided as an argument, it must create a new DFS based on the values of the configuration variables.

After the setup has been completed, you must create a file which stores information about the DFS in dfs_setup_config. This file should store the configuration settings and must be used to load the DFS for later use.

## Initial Setup
The fs_path attribute stores the absolute path to a virtual directory where a user can create folders and add files. However, do remember that this is virtual, and no data is stored here (since this is simply the HDFS root directory). Data is only stored in the Data Nodes in blocks. The Name Node is incharge of mapping file system operations performed in this directory to files present in the Data Nodes. This path is used as a root directory to manipulate the distributed file system.

## Data Nodes
1. The number of Data Nodes to create is given by num_datanodes. You are free to implement a Data Node based on your implementation.
2. All Data Nodes must be created inside the path_to_datanodes directory.
3. Each Data Node must store only datanode_size blocks. If there is no more available space in a datanode, it must be removed from the block replication process.
4. The block size for each file is provided by block_size. Each file must be split into the specified block size, and each block must be replicated replication_factor number of times. Each replicated block must now be stored in a Data Node.
5. Choosing a Data Node for a block must be based on hashing (or any other algorithm). If there are collisions, or a Data Node is full, suitable measures must be taken. The allocation of blocks to Data Nodes must be efficient.
6. All information about the position of each replicated block in a Data Node must be stored on the Name Node.
7. Data Nodes can store logging information (such actions performed on a block, creation of new files and folders, etc) in datanode_log_path. Each Data Node must create its own logging file inside this directory to track only its blocks.

## Name Nodes
1. The primary Name Node must keep track of all changes being made in path_to_fs. This includes creation or deletion of files and folders.
2. Each file in fs_path and its associated blocks must be tracked by the Name Node, which should store the mapping of each block of a file to a particular Data Node.
3. The Name Node should also track details about each file such as the number of blocks it consists of, the location of the Data Nodes where its blocks are located and so on.
4. The Name Node also logs information about Data Nodes. Meta data about Data Nodes (can be any information you wish to track, such as size of each Data Node, number of files, number of blocks occupied, etc) must be logged to namenode_log_path.
5. Each Name Node must periodically (sync_period) send a heartbeat signal to every Data Node. During this process, it must ensure all Data Nodes are running, and update the status of each Data Node. If a Data Node is full, or if it cannot find a Data Node, it must take suitable actions to ensure there isn’t a failure while accessing the DFS.
6. Each Name Node should create a checkpoint of its data every sync_period seconds in namenode_checkpoints.
7. A secondary Name Node must be setup. It should back up all the data stored by the primary Name Node every sync_period seconds. If the primary Name Node ever fails, the secondary Name Node must take over as the primary Name Node and must retrieve all information from the last created checkpoint. It must also create a new secondary Name Node and restore information upto the last checkpoint.

## Persistent Storage
1. All files, folders and logs created in the Data Nodes and Name Nodes must persist to disk.
2. The configuration stored in dfs_setup_config should be used to load the DFS for later use to manipulate on the DFS.
3. It must be possible to load the configuration of multiple DFS configurations, since multiple config files may be used to setup different DFS on the same machine.

## Loading the Distributed File System
1. The absolute path to a dfs_setup_config file will be provided to load the DFS. If such an argument is not provided, it must load the most recently created DFS. If no such DFS exists, it must prompt the user to create a new one.
2. After reading the dfs_setup_config file, it must display the configuration it is trying to load for the user.
3. It should verify if the DFS can be loaded, by verifing the paths of the Data Nodes and Name Nodes. It must ensure and perform basic checks.
4. If the DFS has been loaded for the first time, it must prompt the user to format the Name Node. The format operation must delete all data inside Data Nodes and Name Nodes and delete the contents of all the log files.

## Accessing the Distributed File System
1. Implement a CLI interface that prompts the user for commands
2. The CLI must mandatorily implement the following commands. You are free to add more commands to your CLI as well.
> put, cat, ls, rm, mkdir, rmdir
3. For each command, the corresponding HDFS action must be carried out. For example, if the put command is used, it must also receive two arguments (source_file and destination_directory). The source_file must be split into blocks based on the configuration and each block must be assigned to a Data Node. The Name Node must be updated with the details of the mapping between blocks and Data Nodes and must also contain the virtual path of the source_file inside the fs_path.
4. The CLI should never crash unless the process has been terminated.

## Running Hadoop Jobs
1. From the dfs_setup_config, the configuration of the DFS must be loaded, which in turn will load the configuration of the Name Node and Data Nodes.
2. This should be used to obtain the number of blocks for the --input file. For each block, obtain its contents and combine them in the right order.
3. You need to pass this input through the cat command to your --mapper file, sort the output, feed that to your --reducer file and store the output in the --output directory.
