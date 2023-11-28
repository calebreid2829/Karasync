import threading as th
from threading import Event as Thread_Event
import multiprocessing as mp
from multiprocessing import cpu_count,JoinableQueue,Event
from time import time
import pandas as pd
import queue
from time import sleep
from tqdm import tqdm
from multiprocessing import Lock
import multiprocessing.queues

class Queue(mp.queues.JoinableQueue):
  def __init__(self,maxsize=-1,block=True,timeout=None,**kwargs):
    self.block = block
    self.timeout = timeout
    self.maxsize = maxsize
    self.kwargs = kwargs
    super().__init__(maxsize,ctx=mp.get_context())

  def get(self,block=True,timeout=1):
    values = super().get(block=block,timeout=timeout)
    if values is None: raise queue.Empty
    return values,self.kwargs

class Director:
  def __init__(self,max_cpus=None,max_threads=None):
    if max_cpus is None: max_cpus = cpu_count/2
    if max_threads is None: max_threads = max_cpus * 5
    self.max_cpus = max_cpus
    self.max_threads = max_threads
    self.projects = {}
    self.managers = []

  def new_project(self,function,mode,workers=None,name=None,*args,**kwargs):
    if workers is None:
      count = 1
      for x in self.projects:
        if self.projects[x].mode == mode: count+=1
      if mode == 'thread': workers = self.max_threads/count
      else: workers = self.max_cpus/count
    if name is None:
      name = function.__name__
    project = Project(function,mode,workers,name,*args,**kwargs)
    self.projects[name] = project
    self.hire_manager(project)

  def hire_manager(self,project):
    manager = Manager(project)
    self.managers.append(manager)

  def start_project(self,project,iterable,join=True,**kwargs):
    try:
      manager = self.projects[project]._manager
    except KeyError as e:
      print('Project does not exist')
      print(e)
    return manager.start_project(iterable,join,**kwargs)

class Manager:
  def __init__(self,project):
    self.workers = []
    self.project = project
    self.project._manager = self
    self.lock = Lock()
    self.__worker_input = None
    self.__worker_output = None
    self.__final_output = None

  def update_bar(self,i):
    self.pbar.update(1)
    self.pbar.refresh()
    if self.pbar.n == self.pbar.total:
      sleep(1)
      self.pbar.close()
      self.layoffs()
    return i

  def set_bar(self,maxsize):
    self.pbar = tqdm(total=maxsize, leave=True)
    self.pbar.set_description(self.project.name)
    pbar_worker = Worker('pbar','thread',self.__worker_output,self.__final_output,self.update_bar)
    pbar_worker.Daemon = True
    self.workers.append(pbar_worker)

  def start_project(self,iterable,join=True,**kwargs):
    if type(iterable) == Queue:
      self.__worker_input = iterable
      maxsize = self.__worker_input.maxsize
      self.__worker_input.kwargs = kwargs
    else:
      maxsize=len(iterable)
      self.__worker_input = Queue(maxsize=maxsize,**kwargs)
      for x in iterable:
        self.__worker_input.put(x)
    sleep(1)
    self.__worker_output = Queue(maxsize=maxsize)
    self.__final_output = Queue(maxsize=maxsize)
    self.hire_workers(self.__worker_input,self.__worker_output)
    pbar_worker = self.set_bar(maxsize)
    for worker in self.workers:
      worker.start()
    if not Join: return self.__final_output

    self.__worker_input.join()
    sleep(1)
    results = []
    while not self.__final_output.full():
      sleep(0.01)
    while not self.__final_output.empty():
      results.append(self.__final_output.get())
    return results

  def wrap_up(self):
    self.pbar.close()
    self.layoffs()
    while not self.__worker_input.empty():
      self.__worker_input.get()
    while not self.__worker_output.empty():
      self.__worker_output.get()
    while not self.__final_output.empty():
      self.__final_output.get()

  def hire_workers(self,Input,Output):
    for worker in range(len(self.workers),self.project.workers):
      ID = f'{self.project.name} - Worker {len(self.workers)+1}'
      self.workers.append(Worker(ID,self.project.mode,Input,Output,self.project.target))

  def layoffs(self):
    while self.workers:
      worker = self.workers.pop()
      Manager.fire(worker)

  def fire(worker):
    worker._exit_code.set()
    try:
      if worker.is_alive():
        worker.terminate()
    except AttributeError:
      pass



