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



