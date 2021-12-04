def cat(namenode_path,datanode_path,fs_path,file_path):
    print("hi")
    # extensions = file_path.split('.')[-1]
    # check_path = fs_path + file_path
    # # global content_cat
    # # with open(os.path.join(namenode_path , 'primary.json')) as primary:
    # #     content_cat = json.loads(primary.read())
    # print(check_path)
    # # if(check_path not in content):
    # #     print("File not found")
    # # else:
    # #     print(content_cat)
    #     # print("else")
    #     # current_dict = content[check_path]
    #     # n = len(current_dict)
    #     # for i in range(1,n+1):
    #     #     empty_list = []
    #     #     for key,value in current_dict[i].items():
    #     #         empty_list.append((key,value))
    #     #     data_node = empty_list[0][0]
    #     #     data_node_block = empty_list[0][1]
    #     #     final_path = datanode_path + str(data_node) + '_data_node/' + str(data_node_block) + '.' + extensions
    #     #     print(final_path)
    #         # with open(final_path,"r") as file_read:
    #         #     content_to_display = file_read.read()
    #         #     print(content_to_display,end="")
    #         #     file_read.close()