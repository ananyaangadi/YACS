import socket   
import sys
import threading
import time

# For cross system communication, confgiure accordingly
ip = "127.0.0.1"
master_ip = "127.0.0.1"
execution_pool = {}
#Format: key: task_id, value: duration
##############################
#Logging
#w_log file to log time taken for task execution
log_file = open('w_log.txt','a')
time_start = {}
time_end = {}
##############################
def listen_for_task_launch():
	s = socket.socket()          
	port = int(sys.argv[2])       
	s.bind((ip, port))         
	#Need to buffer connections because tasks might come faster than we can add them to queue
	s.listen(100)	           
	while True: 
		c, addr = s.accept()      
		request = c.recv(1024)
		request = str(request,'utf-8')

		task_id, duration = request.split()
		#####################################
		#execution_pool is a shared data structure between listen_for_task_launch and execute_tasks, hence using lock 
		lock.acquire()
		print("I received task ",request)
		time_start[task_id]=time.time()
		execution_pool[task_id] = int(duration)
		###########################################
		lock.release()
	c.close() 
        
def execute_tasks():
	print("Worker is up")
	while True:
		if len(execution_pool)==0:
			continue
		print("Executing tasks")
		#print("Execution pool right now:")
		#print(execution_pool)
		#Completed tasks must be later removed from the execution pool, these are in to_be_removed
		to_be_removed = []
		#execution_pool is a shared data structure between listen_for_task_launch and execute_tasks, hence using lock 
		lock.acquire()
		for i in execution_pool.keys():
			#Decrementing duration of each task
			execution_pool[i] -= 1
			#Making task sleep for 1 second to simulate a second of execution
			time.sleep(1) #Task running
			#If task completed, log
			if execution_pool[i] == 0:
				#########################
				#Logging
				time_end[i]=time.time()
				#########################
				str_to_write = i + ' ' + str(time_start[i]) + ' ' + str(time_end[i]) +'\n'
				log_file.write(str_to_write)
				log_file.flush()
				#Send update to master
				with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
					s.connect((master_ip, 5001))
					message = sys.argv[1] + " " + i
					print("I am done with ",i)
					s.send(message.encode())
					print("Sent ",message," to master")
				to_be_removed.append(i)
		#Remove completed tasks from execution_pool
		for i in to_be_removed:
			del execution_pool[i]
		lock.release()
    


class TaskLaunch(threading.Thread):
   def run(self):
      listen_for_task_launch()
      
      
class Execute(threading.Thread):
   def run(self):
      execute_tasks()

#Lock for protecting access to shared data structure execution_pool
lock = threading.Lock()     
thread1 = TaskLaunch()
thread2 = Execute()

thread1.start()
thread2.start()
thread1.join()
thread2.join()
log_file.close()       

