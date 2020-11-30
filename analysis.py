import re
m_log = open('m_log.txt','r')
w_log = open('w_log.txt','r')
plot_log = open('plot_log.txt','r')

jobs = []	#job completion times
tasks = []	#task completion times
workers  = {}
for line in w_log:
	splitted = line.split(' ',2)
	t=float(splitted[2])-float(splitted[1])
	tasks.append(t)
for line in m_log:
	splitted = line.split(' ',2)
	t=float(splitted[2])-float(splitted[1])
	jobs.append(t)

	
for line in plot_log:
	worker_id, size_execution_pool = line.split(' ',1)
	worker_id = int(worker_id)
	if worker_id not in workers:
		workers[worker_id] = []
	workers[worker_id].append(int(size_execution_pool))

from statistics import mean
from statistics import median
print("Mean time to execute tasks: ",mean(tasks))
print("Median time to execute tasks: ",median(tasks))
print("Mean time to execute jobs: ",mean(jobs))
print("Median time to execute jobs: ",median(jobs))

	
import matplotlib.pyplot as plt
for i in workers.keys():
	plt.figure()
	plt.plot(workers[i])
	plt.xlabel(i)
	plt.ylabel("Workload over time")
	plt.show()
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	


