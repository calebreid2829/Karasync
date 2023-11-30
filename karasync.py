import threading as th
from threading import Event as Thread_Event
import multiprocessing as mp
from multiprocessing import cpu_count,JoinableQueue,Event
from time import time
import pandas as pd
import queue
from time import sleep
from tqdm.auto import tqdm
from multiprocessing import Lock
import multiprocessing.queues

class Queue(mp.queues.JoinableQueue):
  def __init__(self,maxsize=-1,block=True,timeout=None):
    self.block = block
    self.timeout = timeout
    self.maxsize = maxsize
    super().__init__(maxsize,ctx=mp.get_context())

  def get(self,block=True,timeout=1):
    values = super().get(block=block,timeout=timeout)
    if values is None: raise queue.Empty
    #if len(self.kwargs) > 0: values = (values,self.kwargs)
    return values

class Manager:
    def __init__(self,mode,target,workers,iterable,**kwargs):
        self.target = target
        self.lock = Lock()
        self.worker_input = self.set_input(iterable)
        self.__worker_output = Queue(maxsize=self.worker_input.maxsize)
        self.__final_output = Queue(maxsize=self.worker_input.maxsize)
        self.workers = self.hire_workers(workers,mode,self.worker_input,self.__worker_output,target)

    def set_input(self,iterable):
        if type(iterable) == Queue:
          worker_input = iterable
          maxsize = worker_input.maxsize
        else:
          maxsize=len(iterable)
          worker_input = Queue(maxsize=maxsize)
          for x in iterable:
            worker_input.put(x)       
        sleep(1)
        return worker_input
    
    def set_bar(self,maxsize):
        self.pbar = tqdm(total=maxsize, leave=True)
        self.pbar.set_description(self.target.__name__)

    def update_bar(self):
        try:
            while True:
                i = (yield)
                self.pbar.update(i)
                self.pbar.refresh()
                if self.pbar.n == self.pbar.total:
                    pass
                    #sleep(1)
                    #self.pbar.close()
                    #self.layoffs()
        except GeneratorExit: 
            pass
    
    def start_project(self,join=True):
        self.set_bar(self.worker_input.maxsize)
        upd_bar = self.update_bar()
        upd_bar.__next__()
        for worker in self.workers:
            worker.bar = upd_bar
            worker.start()
        if not join: return self.__worker_output
        
        self.worker_input.join()
        results = []
        while not self.__worker_output.full():
            sleep(0.01)
        while not self.__worker_output.empty():
            results.append(self.__worker_output.get())
        sleep(.25)
        upd_bar.close()
        self.wrap_up()
        return results
    
    def wrap_up(self):
        self.pbar.close()
        self.layoffs()
        while not self.worker_input.empty():
          self.worker_input.get()
        while not self.__worker_output.empty():
          self.__worker_output.get()
    
    def hire_workers(self,workers,mode,Input,Output,target):
        final_workers = []
        for worker in range(workers):
            ID = f'{target.__name__} - Worker {worker+1}'
            final_workers.append(Worker(ID,mode,Input,Output,target))

        return final_workers
        
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
        self.bar = None

    

    def run(self):
        while not self._exit_code.is_set():
          try:
            vals = self._Input.get(block=True,timeout=1)
            if type(vals) != tuple:
                result = self.target(vals)
                self._Output.put(result,timeout=5)
                self._Input.task_done()
            else:
                args = vals[0]
                kwargs = vals[1]
                result = self.target(args,**kwargs)
                self._Output.put(result,timeout=5)
                self._Input.task_done()
            try:
                self.bar.send(1)
            except ValueError:
                pass
          except queue.Empty as e:
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

def test(num):
  return num * 5

def test2(nums):
  num1 = nums[0]
  num2 = nums[1]
  return num1 *-num2

li = [x for x in range(10)]
li2 = [x for x in range(10,20)]

kamiya = Manager('thread',test,4,li)
#kamiya.new_project(test2,'proc')
results1 = kamiya.start_project(join=True)
#results = kamiya.start_project('test2',(results1,li2))
results1



