import json
from datetime import datetime
import time
import sys
checker_count = 0
check_point_period = 1
sync_period = 5



  

path_to_pnn = sys.argv[1]
nn_check_path = sys.argv[2]

def checker(path_to_pnn,nn_check_path):
    
    try:
        with open(path_to_pnn + '/primary.json') as pri_file:
            pri_file.close()
        with open(nn_check_path+'/Checkpoints.txt','a') as ch_pt:
            ch_pt.write(f'{datetime.now()} primary namenode HEART BEAT checked\n')
            ch_pt.close()
    except:
        with open(nn_check_path+'/Checkpoints.txt','a') as ch_pt:
            ch_pt.write(f'{datetime.now()} primary namenode HEART BEAT failed!\nRecovery in Progress...\n\n')
            ch_pt.close()
            
        with open(path_to_pnn + '/secondary.json','r') as se_file:
            with open(path_to_pnn+'/primary.json','w') as pri_file:
                json.dump(json.loads(se_file.read()),pri_file)
                pri_file.close()
            se_file.close()
            
        with open(nn_check_path+'/Checkpoints.txt','a') as ch_pt:
            ch_pt.write(f'{datetime.now()} primary namenode Recovered and running...!\n')
            ch_pt.close()
            
    finally:
        return 1
    
def sync(path_to_pnn,nn_check_path):
    try:
        with open(path_to_pnn+'/primary.json','r') as pri_file:
            with open(path_to_pnn+'/secondary.json','w') as se_file:
                se_file.seek(0)
                se_file.truncate()
                json.dump(json.loads(pri_file.read()),se_file)
                se_file.close()
        pri_file.close()

        with open(nn_check_path+'/Checkpoints.txt','a') as ch_pt:
            ch_pt.write(f'{datetime.now()} Primary and Secondary Name node sycned!...\n\n')
            ch_pt.close()
    except:
        with open(nn_check_path+'/Checkpoints.txt','a') as ch_pt:
            ch_pt.write(f'{datetime.now()} Waiting for Primary Node to get active again...!\n\n')
            ch_pt.close()

while True:
    time.sleep(check_point_period)
    checker(path_to_pnn,nn_check_path)
    checker_count += check_point_period
    
    if(checker_count == sync_period):
        sync(path_to_pnn,nn_check_path)
        checker_count = 0


# def delete_check(path_to_nn,check_path):
#     try:
#         with open(path_to_nn+'/primary.json') as pri_file:
#             pri_file()
#         with open(check_path+'Checkpoints.txt','a') as ch_pt:
#             ch_pt.write(f'{datetime.now()} primary namenode HEART BEAT checked\n')
#     except:
#         return 1 #1 implies deleted or crash occured
    
# def copy_ss_to_new_nn(path_to_nn,check_path):
#     status = delete_check(path_to_nn,check_path)
#     if status:
#         with open(path_to_nn+'/secondary.json','r') as se_file:
#             with open(path_to_nn+'/primary.json','w')
        
            
        