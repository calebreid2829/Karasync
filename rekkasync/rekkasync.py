import threading as th
from threading import Event as Thread_Event
import multiprocessing as mp
from multiprocessing import cpu_count,JoinableQueue,Event
from time import time
import pandas as pd
import queue
from time import sleep
from tqdm.auto import tqdm
from multiprocessing import Lock, Pipe
import multiprocessing.queues
from functools import partial

class Queue(mp.queues.JoinableQueue):
    def __init__(self,maxsize=-1,block=True,timeout=None,manager = None,pbar = False):
        self.block = block
        self.timeout = timeout
        self.maxsize = maxsize
        self.manager = manager
        self.count = 0
        if pbar: self.set_bar()
        else: self.pbar = False
        super().__init__(maxsize,ctx=mp.get_context())

    def set_bar(self):
        self.pbar = tqdm(total=self.maxsize, leave=True)

    def get(self,block=True,timeout=1):
        values = super().get(block=block,timeout=timeout)
        if values is None: raise queue.Empty
        return values

    def update_bar(self):
        self.pbar.update(1)
        self.pbar.refresh()
        self.count += 1
        sleep(0.01)
        if self.count >= self.maxsize:
            self.pbar.close()

    def put(self,result,timeout=5):
        super().put(result,timeout)
        if self.pbar: self.update_bar()
    

class Manager:
    def __init__(self,workers,mode,target):
        if type(target) == tuple:
            self.target = partial(target[0],**target[1])
        else:
            self.target = target
        self.worker_input = None
        self.__worker_output = None
        self.workers = None
        self.hire_workers(workers,mode)

    def set_pipeline(self,iterable,pbar):
        if type(iterable) == Queue:
          worker_input = iterable
        else:
          maxsize=len(iterable)
          worker_input = Queue(maxsize=maxsize,manager=self)
          for x in iterable:
            worker_input.put([x]) 
        sleep(1)
        self.worker_input = worker_input
        self.__worker_output = Queue(maxsize=self.worker_input.maxsize,manager=self,pbar=pbar)
    
    def start_project(self,iterable,join=True,pbar=False):
        self.set_pipeline(iterable,pbar)
        for worker in self.workers:
            worker._Input = self.worker_input
            worker._Output = self.__worker_output
            worker.start()
        if not join: return self.__worker_output
        
        self.worker_input.join()
        results = []
        while not self.__worker_output.full():
            sleep(0.01)
        while not self.__worker_output.empty():
            results.append(self.__worker_output.get())
        sleep(.25)
        self.wrap_up()
        return results
    
    def wrap_up(self):
        self.layoffs()
        while not self.worker_input.empty():
          self.worker_input.get()
        while not self.__worker_output.empty():
          self.__worker_output.get()
    
    def hire_workers(self,workers,mode):
        self.workers = []
        for worker in range(workers):
            ID = f'Worker {worker+1}'
            self.workers.append(Worker(ID,mode,self.target))
        
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
    def __init__(self,ID,Input=None,Output=None,target=None,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.ID = ID
        self._Input = Input
        self._Output = Output
        self.target = target
        self._status = 'idle'

    def get(self):
        pass

    def run(self):
        while not self._exit_code.is_set():
            try:
                vals = self._Input.get(block=True,timeout=1)
                result = self.target(*vals)
                self._Output.put(result,timeout=5)
                self._Input.task_done()
            except queue.Empty as e:
                pass
            except Exception as e:
                print(e)
                self._exit_code.set()
            

    def terminate(self):
        super().terminate()

class Thread_Worker(Worker,th.Thread):
  def __init__(self,ID,Input=None,Output=None,target=None):
    super().__init__(ID,Input,Output,target)
    self._exit_code = Thread_Event()
    self.Daemon = True

class Process_Worker(Worker,mp.Process):
    def __init__(self,ID,Input=None,Output=None,target=None):
        super().__init__(ID,Input,Output,target)
        self._exit_code = Event()
        self.Daemon = True

def Worker(ID,mode,target):
  if mode == 'thread': return Thread_Worker(ID,target=target)
  else: return Process_Worker(ID,target=target)