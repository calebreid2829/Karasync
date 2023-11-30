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

class Worker:
  def __init__(self,ID,Input,Output,target=None,*args,**kwargs):
    super().__init__(*args,**kwargs)
    self.ID = ID
    self._Input = Input
    self._Output = Output
    self.target = target
    self._status = 'idle'
    pass

  def run(self):
    while not self._exit_code.is_set():
      try:
        vals = self._Input.get(block=True,timeout=1)
        if type(vals) != tuple:
          result = self.target(vals)
        else:
          args = vals[0]
          kwargs = vals[1]
          result = self.target(args,**kwargs)
          self._Output.put(result,timeout=5)
          self._Input.task_done()
      except queue.Empty:
        pass

  def terminate(self):
    super().terminate()

class Thread_Worker(Worker,th.Thread):
  def __init__(self,ID,Input,Output,target=None):
    super().__init__(ID,Input,Output,target)
    self._exit_code = Thread_Event()
    self.Daemon = True

class Process_Worker(Worker,mp.Process):
  def __init__(self,ID,Input,Output,target=None):
    super().__init__(ID,Input,Output,target)
    self._exit_code = Event()
    self.Daemon = True

class Project:
  def __init__(self,target,mode,workers,name,manager=None,*args,**kwargs):
    self.name = name
    self.target = target
    self.mode = mode
    self.workers = int(workers)
    self.status = 'idle'
    self._manager = manager
    self._args = args
    self._kwargs = kwargs

  def layoffs(self,workers):
    self.workers = workers
    self._manager.layoffs()

  def budget_increase(self,workers):
    self.workers = workers
    self._manager.hire_workers()

def __retrieve_var_name(var):
  callers_local_vars = inspect.currentframe().f_back.f_back.f_back.f_back.f_locals.items()
  return [var_name for var_name, var_val in callers_local_vars if var_val is var]

def __set_args(func,iterable,**kwargs):
  func_names = __retrieve_var_name(iterable)
  final_args = []
  for y in iterable:
    li = []
    for x in func.__code__.co_varnames:
      if x in kwargs: li.append(kwargs[x])
      elif x in func_names: li.append(y)
    final_args.append(li)
  return final_args

def Worker(ID,mode,Input,Output,target=None):
  if mode == 'thread': return Thread_Worker(ID,Input,Output,target)
  else: return Process_Worker(ID,Input,Output,target)

def new_queue(mode,max_size):
  if mode == 'threads': return queue.Queue()
  else: return Queue()

def test(num,num2):
  return num * num2

def test2(nums):
  num1 = nums[0]
  num2 = nums[1]
  return num1 *-num2

li = [x for x in range(10)]
li2 = [x for x in range(10,20)]

kamiya = Director(4,4)
kamiya.new_project(test,'thread')
kamiya.new_project(test2,'proc')
results1 = kamiya.start_project('test',li,join=False,num2=5)
results = kamiya.start_project('test2',(results1,li2))
results



