# YACS - Yet Another Centralized Scheduler
Final Project for the course Big Data (UE18CS322)

# Members
Parul S Kumar, Ananya Angadi, Sakshi, Nidhi Patil

# Overview
YACS simulates a centralized scheduler. In this implementation, the master listens for incoming job requests. Once it receives a job, it delegates corresponding tasks to the workers. The worker simulates task execution, and updates worker on completion. Multithreading is used extensively to ensure parallel execution of the following long running jobs - listening for requests, delegating tasks, execution of tasks and listening for updates. Locks are used to ensure that critical sections are protected and thread execution is synchronized. The start and end times of both jobs as well as individual tasks are logged at appropriate times for later analysis. The framework also provides support for 3 scheduling algorithms: Round Robin, Least Loaded, and Random.

# Code

config.json: contains worker configuration information - including worker_id, number of slots, and port number it listens on
requests.py: generates job requests with exponentially distributes inter-arrival time and sends them to master
master_latest.py:
Contains 5 threads:

Listen for job requests - accepts requests and adds them to the job queue, we do not process them as they come in, as that can cause requests to pile up
Start maps - it removes a job from the queue, initializes corresponding data structures, and readies all the map tasks for dispatch
Dispatch tasks - it first finds a free worker slot according to the scheduling algorithm specified (Round Robin, Least loaded, and Random), and then dispatches task to said worker
Listen for worker updates - worker sends messages specifying completed tasks, this thread checks whether the completed task satisfies any dependencies, and starts reducers accordingly
Monitor tasks - logs the workload at each worker every wall clock second

request.py:
Listen for task launch - accept task from worker and add to execution pool
Execute tasks - subtract one from the duration of all tasks in execution pool, and update master if any tasks finish executing



# Instructions to Run

1) To run master_latest.py: python3 master.py config.json RR  [last argument is scheduling algorithm]
2) To run worker_latest.py: python3 worker.py 1 4ooo [worker_id and port_number]
  To run worker_latest.py: python3 worker.py 2 4001
  To run worker_latest.py: python3 worker.py 3 4002
3) To run requests.py: python3 requests.py 10 [number of requests]

For more information, refer BD_YACS_doc.pdf
