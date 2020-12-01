import socket   
import sys
import threading
import time

ip = "127.0.0.1"
master_ip = "127.0.0.1"
execution_pool = {}
##############################3
log_file = open('w_log.txt','a')
time_start = {}
time_end = {}
##############################
def listen_for_task_launch():
	s = socket.socket()          
	port = int(sys.argv[2])       
	s.bind((ip, port))         
	s.listen(5)           
	while True: 
		c, addr = s.accept()      
		request = c.recv(1024)
		request = str(request,'utf-8')
		print("I received task ",request)
		task_id, duration = request.split()
		#####################################
		lock.acquire()
		time_start[task_id]=time.time()
		execution_pool[task_id] = int(duration)
		###########################################
		lock.release()
	c.close() 
        
def execute_tasks():
	print("Executing tasks")
	while True:
		if len(execution_pool)==0:
			continue
		print("Execution pool right now:")
		print(execution_pool)
		to_be_removed = []
		lock.acquire()
		for i in execution_pool.keys():
			execution_pool[i] -= 1
			time.sleep(1) #!#
			if execution_pool[i] == 0:
				#########################
				time_end[i]=time.time()
				########################
				str_to_write = i + ' ' + str(time_start[i]) + ' ' + str(time_end[i]) +'\n'
				log_file.write(str_to_write)
				log_file.flush()
				
				with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
					s.connect((master_ip, 5001))
					message = sys.argv[1] + " " + i
					print("I am done with ",i)
					s.send(message.encode())
					print("Sent ",message," to master")
				to_be_removed.append(i)
		for i in to_be_removed:
			del execution_pool[i]
		lock.release()
    


class TaskLaunch(threading.Thread):
   def run(self):
      listen_for_task_launch()
      
      
class Execute(threading.Thread):
   def run(self):
      execute_tasks()
      
lock = threading.Lock()     
thread1 = TaskLaunch()
thread2 = Execute()

thread1.start()
thread2.start()
thread1.join()
thread2.join()
log_file.close()       

