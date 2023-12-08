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
