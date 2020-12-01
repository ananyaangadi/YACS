import socket                
import json
import sys
import threading
import ast
import time
import random

#This is to ensure cross system communication, change ip accordingly.
ip = "127.0.0.1"
# worker ip should be mentioned in config for cross system communication
worker_ip = "127.0.0.1"

job_map = {}
#Format: key = job_id, value = list of map tasks 
job_reduce = {}
#Format: key = job_id, value = list of reduce tasks 
completed_map = {}
#Format: key = job_id, value = set of completed map tasks 
completed_reduce = {}
#Format: key = job_id, value = set of completed reduce tasks 
fp = open(sys.argv[1])
#open config
config = json.loads(fp.read())
workers = config['workers']
print('Workers from config :')
print(workers)
################################################
#Opening Log file

# logs to record time taken for job completion
log_file = open("m_log.txt","a")
################################################
worker_slots = {}
#Format: key = worker_id, value = total number of slots 
worker_slots_remaining = {}
#Format: key = worker_id, value = no of remaining slots
worker_ports = {}
#Format: key = worker_id, value = port no

# populate the dictionaries
for i in workers:
    worker_slots[i['worker_id']] = i['slots']
    worker_slots_remaining[i['worker_id']] = i['slots']
    worker_ports[i['worker_id']] = i['port']

#print(worker_slots)
#print(worker_slots_remaining)
num_workers = len(worker_slots) # number of workers
latest_worker_checked = -1 # for round robin scheduling
#print(latest_worker_checked)
ordered_worker_slots = sorted(worker_slots_remaining.items()) # for round robin scheduling
#print(ordered_worker_slots)
#Format: List of tuples having worker_id, slots 

#Scheduling Algorithms
def round_robin():
	#Global because has to be shared among multiple iterations of the function
	global latest_worker_checked
	if latest_worker_checked == -1:
		i = 0
	else:
		i = (latest_worker_checked + 1)%num_workers

	while i < num_workers:
		#print("checking")
		#Worker_slots_remaining is a shared data structure hence using lock
		#Finding which worker to use must be done atomically		
		lock.acquire()
		#If a free slot is found, use that worker
		if worker_slots_remaining[ordered_worker_slots[i][0]] > 0:
			lock.release()
			latest_worker_checked = i
			return ordered_worker_slots[i][0]
		lock.release()	
		#Look for free slots in a round robin fashion
		i = (i+1)%num_workers
	return -1

def least_loaded():
	while True:
		i = 0
		#Worker_slots_remaining is a shared data structure hence using lock
		#Finding which worker to use must be done atomically
		lock.acquire()
		keys = list(worker_slots_remaining.keys())
		#print(keys)
		values = list(worker_slots_remaining.values())
		#print(values)
		maxx = 0
		ll = 0
		#Look for worker with maximum remaining slots
		for i in range(len(values)):
			#Check if there are free slots
			if values[i] > maxx:
				maxx = values[i]
				ll = keys[i]
		if maxx != 0:
			lock.release()
			return ll
		lock.release()
	return -1	
	
def random_scheduler():
	while True:
		#Worker_slots_remaining is a shared data structure hence using lock
		#Finding which worker to use must be done atomically
		lock.acquire()
		keys = list(worker_slots_remaining.keys())
		#print(worker_slots_remaining)
		#Randomly selecting a worker to use
		r =random.randrange(0,num_workers)
		#Check if there are free slots
		if worker_slots_remaining[keys[r]] > 0:
			lock.release()
			return keys[r]
		lock.release()
	return -1
	
	
sch = sys.argv[2]

if sch == 'RR':
	scheduling_algo = round_robin
	
elif sch == 'LL':
	scheduling_algo = least_loaded

elif sch == 'R':
	scheduling_algo = random_scheduler
	
####################################
#Time tracking using dictionaries
#Logging
time_start = {}
time_end = {}
request_q = []
#Request_q is a queue of incoming job requests 
####################################
def listen_for_requests():
	s1 = socket.socket()          
	port = 5000      
	s1.bind((ip, port))         
	#Need to buffer connections because requests might come faster than we can add them to queue
	s1.listen(5)           
	while True: 
		print("listening for requests")
		c, addr = s1.accept() 
		request = c.recv(1024)
		req = str(request,'utf-8')
		#Request queue is shared between start_maps thread and listen_for_requests thread
		#Adding to queue must be an atomic operation
		req_q_lock.acquire()
		request_q.append(req)
		req = ast.literal_eval(req)
		##############################################
		#Logging
		time_start[req['job_id']]=time.time()
		##############################################
		req_q_lock.release()
	c.close() 

def start_maps():
	while True:
		if len(request_q) == 0:
			continue
		#print(request_q)
		req = request_q[0]
		#Request queue is shared between start_maps thread and listen_for_requests thread
		#Removing from queue must be an atomic operation
		req_q_lock.acquire()
		request_q.pop(0)
		req_q_lock.release()
		req = ast.literal_eval(req)
		#print(req)
		#Initialization of the dictionaries
		job_map[req['job_id']] = req['map_tasks']
		job_reduce[req['job_id']] = req['reduce_tasks']
		completed_map[req['job_id']] = set()
		completed_reduce[req['job_id']] = set()
		#Starting all the map jobs 
		for i in req['map_tasks']:
			thread3 = DispatchTask(i['task_id'],i['duration'])
			thread3.start()
			thread3.join()
        		
		
update_q = []
def listen_for_worker_updates():
	s = socket.socket()          
	port = 5001        
  
	s.bind((ip, port))         
	#Need to buffer connections because requests might come faster than we can add them to queue
	s.listen(200) 	#There is a system limit anyway
	while True: 
		print("listening for updates ")
		#print(worker_slots_remaining)
		c, addr = s.accept()     
		request = c.recv(1024)
		print("Received update: ",str(request,'utf-8')) 
		request = str(request,'utf-8')
		#Update queue is shared between listen_for_worker_updates thread and check_dependency thread
		#Adding to queue must be an atomic operation
		upd_q_lock.acquire()
		update_q.append(request)
		upd_q_lock.release()
		worker_id,completed_task = request.split()
		#Worker_slots_remaining is a shared data structure hence using lock
		lock.acquire()
		worker_slots_remaining[int(worker_id)] += 1
		#print(worker_slots_remaining)
		lock.release() 
		#print(update_q)
        			
def check_dependency():
	while True:
		#Update queue is shared between listen_for_worker_updates thread and check_dependency thread
		#Removing from queue must be an atomic operation
		upd_q_lock.acquire()
		if len(update_q)==0:
			upd_q_lock.release()
			continue
		#print(update_q)
		# get element from front of queue
		request = update_q[0]
		update_q.pop(0)
		upd_q_lock.release()
		
		worker_id,completed_task = request.split()
		job_id,task = completed_task.split("_",1)
		# if the completed task is a map task, check for dependencies
		if task[0] == 'M':
			# add completed task to the set corresponding to its job in job_map
			completed_map[job_id].add(completed_task)
			# if all map tasks for that job are over, start all reduce tasks
			if len(completed_map[job_id]) == len(job_map[job_id]):
				for i in job_reduce[job_id]:
					thread4 = DispatchTask(i['task_id'],i['duration'])
					thread4.start()
					thread4.join()
		################################
		# if the completed task is a reduce task, no dependencies, check if job done
		elif task[0] == 'R':
			# add completed task to the set corresponding to its job in job_reduce
			completed_reduce[job_id].add(completed_task)
			# if all reduce tasks for that job are over, job is done
			if len(completed_reduce[job_id]) == len(job_reduce[job_id]):
				time_end[job_id] = time.time()
				# log the time taken for completion of job
				str_to_write = job_id + ' ' + str(time_start[job_id]) + ' ' + str(time_end[job_id]) + '\n'
				log_file.write(str_to_write)
				log_file.flush()
        	################################

       
class AcceptRequest(threading.Thread):
   def run(self):
      listen_for_requests()
      
      
class ListenWorker(threading.Thread):
   def run(self):
      listen_for_worker_updates()
      
class StartMaps(threading.Thread):
   def run(self):
      start_maps()     
      
class CheckDependencies(threading.Thread):
   def run(self):
      check_dependency()

class DispatchTask(threading.Thread):
	def __init__(self,task_id,duration):
		threading.Thread.__init__(self)
		self.task_id = task_id
		self.duration = duration
	def run(self):
		# select a worker based on the prescribed scheduling algorithm
		# scheduling_algo returns worker_id
		worker_id = scheduling_algo()
		#Worker_slots_remaining is a shared data structure hence using lock
		lock.acquire()
		print("Sending task ",self.task_id,"to worker ",worker_id)
		# decrement free slots for that worker
		worker_slots_remaining[worker_id] -= 1
		#print(worker_slots_remaining)
		lock.release()
		
		# dispatch task to worker
		s2 = socket.socket()
		port1 = worker_ports[worker_id]
		# Format of request: task_id duration
		request = str(self.task_id) + " " + str(self.duration)
		s2.connect((worker_ip,port1))
		s2.send(request.encode())


# Monitoring thread created exclusively for logging
# Logs the workload of each worker every wall clock second		
plot_log = open('plot_log.txt','a')
#plot_log file to maintain workload every wall clock second for plotting
class MonitorThread(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)

	def run(self):
		while(1):
			time.sleep(1)
			#Worker_slots_remaining is a shared data structure hence using lock
			lock.acquire()
			for i in list(worker_slots.keys()):
				# no of occupied slots
				var = worker_slots[i]-worker_slots_remaining[i]
				#print(worker_slots[i],'\n',worker_slots_remaining[i])
				# write to log file
				plot_log.write(str(i)+' '+str(var)+ '\n')
				plot_log.flush()
			lock.release()
			
	
# Lock for the data structure worker_slots_remaining
# Ensure that allocation and deallocation of slots is done in an atomic manner
lock = threading.Lock() 
req_q_lock = threading.Lock()   
upd_q_lock = threading.Lock()
thread1 = AcceptRequest()
thread2 = ListenWorker()
thread5 = StartMaps()
thread6 = CheckDependencies()
thread7 = MonitorThread()


thread1.start()
thread2.start()
thread5.start()
thread6.start()
thread7.start()

thread1.join()
thread2.join()
thread5.join()
thread6.join()
thread7.join()
log_file.close()
plot_log.close()
