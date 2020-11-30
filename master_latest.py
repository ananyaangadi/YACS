import socket                
import json
import sys
import threading
import os
import ast
import time
import random
from queue import Queue

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
print(workers)
################################################
#Opening Log file
log_file = open("m_log.txt","a")
##############################################
worker_slots = {}
#Format: key = worker_id, value = total number of slots 

worker_slots_remaining = {}
#Format: key = worker_id, value = no of remaining slots
worker_ports = {}
#Format: key = worker_id, value = port no
for i in workers:
    #os.system("python3 worker.py "+str(i['worker_id'])+" "+str(i['port']))
    #HOW TO START WORKERS?
    worker_slots[i['worker_id']] = i['slots']
    worker_slots_remaining[i['worker_id']] = i['slots']
    worker_ports[i['worker_id']] = i['port']

#print(worker_slots)
print(worker_slots_remaining)
num_workers = len(worker_slots) # number of workers
latest_worker_checked = -1 # for round robin scheduling
print(latest_worker_checked)
ordered_worker_slots = sorted(worker_slots_remaining.items()) # for round robin scheduling
#print(ordered_worker_slots)
#Format: List of tuples having 
#PROBLEM: (Worker_id,#slots) works!
def round_robin():
	global latest_worker_checked
	if latest_worker_checked == -1:
		i = 0
	else:
		i = (latest_worker_checked + 1)%num_workers

	while i < num_workers:
		#print("checking")
		lock.acquire()
		if worker_slots_remaining[ordered_worker_slots[i][0]] > 0:
			lock.release()
			latest_worker_checked = i
			return ordered_worker_slots[i][0]
		lock.release()	
		i = (i+1)%num_workers
	return -1

def least_loaded():
	while True:
		i = 0
		lock.acquire()
		keys = list(worker_slots_remaining.keys())
		print(keys)
		values = list(worker_slots_remaining.values())
		print(values)
		maxx = 0
		ll = 0
		for i in range(len(values)):
			if values[i] > maxx:
				maxx = values[i]
				ll = keys[i]
		if maxx != 0:
			return ll
		lock.release()
	return -1	
	
def random_scheduler():
	while True:
		lock.acquire()
		keys = list(worker_slots_remaining.keys())
		#print(worker_slots_remaining)
		r =random.randrange(0,num_workers)

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
	
###########################
#Time tracking
time_start = {}
time_end = {}
request_q = []

###########################
def listen_for_requests():
	s1 = socket.socket()          
	port = 5000      
	s1.bind((ip, port))         
	s1.listen(5)           
	while True: 
		print("listening for requests")
		c, addr = s1.accept() 
		request = c.recv(1024)
		req = str(request,'utf-8')
		req_q_lock.acquire()
		request_q.append(req)
		req = ast.literal_eval(req)
		##############################################
		time_start[req['job_id']]=time.time()
		##############################################
		req_q_lock.release()
	'''
        req = ast.literal_eval(req)
        ##############################################
        time_start[req['job_id']]=time.time()
        ##############################################
        job_map[req['job_id']] = req['map_tasks']
        job_reduce[req['job_id']] = req['reduce_tasks']
        completed_map[req['job_id']] = set()
        completed_reduce[req['job_id']] = set()
        for i in req['map_tasks']:
        	thread3 = DispatchTask(var['task_id'],var['duration'])
        	thread3.start()
        	thread3.join()
	'''	
	c.close() 

def start_maps():
	while True:
		if len(request_q) == 0:
			continue
		#print(request_q)
		req = request_q[0]
		req_q_lock.acquire()
		request_q.pop(0)
		req_q_lock.release()
		req = ast.literal_eval(req)
		print("REEQ")
		#print(req)
		job_map[req['job_id']] = req['map_tasks']
		job_reduce[req['job_id']] = req['reduce_tasks']
		completed_map[req['job_id']] = set()
		completed_reduce[req['job_id']] = set()
		for i in req['map_tasks']:
			print("Dispatching ",i['task_id'])
			thread3 = DispatchTask(i['task_id'],i['duration'])
			thread3.start()
			thread3.join()
        		
		
update_q = []
def listen_for_worker_updates():
	s = socket.socket()          
	port = 5001        
  
	s.bind((ip, port))         
	s.listen(200) 	##           
	while True: 
		print("listening for updates")
		print(worker_slots_remaining)
		c, addr = s.accept()     
		request = c.recv(1024)
		print("Received update: ",str(request,'utf-8')) 
		request = str(request,'utf-8')
		upd_q_lock.acquire()
		update_q.append(request)
		upd_q_lock.release()
		worker_id,completed_task = request.split()
		#print("Entering critical section")
		lock.acquire()
		worker_slots_remaining[int(worker_id)] += 1
		print(worker_slots_remaining)
		lock.release() 
		#print("Exiting critical section")
		print(update_q)
        			
def check_dependency():
	while True:
		upd_q_lock.acquire()
		if len(update_q)==0:
			upd_q_lock.release()
			continue
		print(update_q)
		request = update_q[0]
		update_q.pop(0)
		upd_q_lock.release()
		worker_id,completed_task = request.split()

		job_id,task = completed_task.split("_",1)
		#print("Entering critical section")
		#print("Exiting critical section")
		if task[0] == 'M':
			completed_map[job_id].add(completed_task)
			if len(completed_map[job_id]) == len(job_map[job_id]):
				for i in job_reduce[job_id]:
					thread4 = DispatchTask(i['task_id'],i['duration'])
					thread4.start()
					thread4.join()
		################################
		elif task[0] == 'R':
			completed_reduce[job_id].add(completed_task)
			if len(completed_reduce[job_id]) == len(job_reduce[job_id]):
				time_end[job_id] = time.time()
				str_to_write = job_id + ' ' + str(time_start[job_id]) + ' ' + str(time_end[job_id]) + '\n'
				log_file.write(str_to_write)
				log_file.flush()
        	###############################

       
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
		worker_id = scheduling_algo()
		lock.acquire()
		print("Task",self.task_id,"sent to worker ",worker_id)
		worker_slots_remaining[worker_id] -= 1
		print("After starting new")
		print(worker_slots_remaining)
		lock.release()
		#time.sleep(1)
		s2 = socket.socket()
		port1 = worker_ports[worker_id]
		request = str(self.task_id) + " " + str(self.duration)
		s2.connect((worker_ip,port1))
		s2.send(request.encode())
		print("Task ",request," sent to worker",worker_id)
		
plot_log = open('plot_log.txt','a')
class MonitorThread(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)

	def run(self):
		while(1):
			time.sleep(1)
			lock.acquire()
			for i in list(worker_slots.keys()):
				var = worker_slots[i]-worker_slots_remaining[i]
				print(worker_slots[i],'\n',worker_slots_remaining[i])
				plot_log.write(str(i)+' '+str(var)+ '\n')
				plot_log.flush()
			lock.release()
			
	

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
