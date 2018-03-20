import time,os, sys, watchdog
import thread
import threading
from watchdog.observers import Observer  
from watchdog.events import PatternMatchingEventHandler
import re
try:
    import queue
except ImportError:
    import Queue as queue

from service import Service

#
# a service class that will 'watch' an inbox folder structure
# using the whatchdog package, which work at inode level
#
# A queue will contains the path of file created in the inbox
# the queue item will be processed one at a time by the worker.processSingleProduct()
#
class WatchdogWatcher(Service):
    # are dictionnaries: {"mission':'value', ...}
    SETTING_INBOX='INBOX'
    SETTING_INCLUSIONS='INCLUSIONS'
    SETTING_EXCLUSIONS='EXCLUSIONS'

    debug=False

    #
    # class init
    # call super class
    #
    def __init__(self, name=None):
        Service.__init__(self, name)
        self.inbox=None
        #
        self.exclusions=None
        self.inclusions = None
        self.watching=False
        self.workqueue=None
        #
        self.counter=0

    #
    # init
    # call super class
    #
    # param: p is usually the path of a property file
    #
    def init(self, p=None, ingester=None):
        Service.init(self, p, ingester)
        self.my_init()
        self.observer={}
        

    #
    # init done after the properties are loaded
    # do:
    # - check if DEBUG option set
    #
    def my_init(self, proxy=0, timeout=5):
        if self.debug:
            print(" WatchdogWatcher init:%s" % self.dumpProperty())
        
        # DEBUG setting
        d=self.getProperty(self.SETTING_DEBUG)
        if d is not None:
            print(" DEBUG setting:%s" % d)
            self.useDebugConfig(d)

        # inbox
        if self.getProperty(self.SETTING_INBOX) is not None:
            if self.debug:
                print("inbox setting:%s" % self.getProperty(self.SETTING_INBOX))
            self.inbox = eval(self.getProperty(self.SETTING_INBOX))
            if self.debug:
                print("@@@@@@@@@@@@@@@@ self.inbox:%s" % self.inbox)

        # exclusionsDict
        if self.getProperty(self.SETTING_EXCLUSIONS) is not None:
            tmp = self.getProperty(self.SETTING_EXCLUSIONS)
            print(" exclusionsDict setting:%s" % tmp)
            if tmp is not None:
                self.exclusions=eval(tmp)
                if self.debug:
                    print("@@@@@@@@@@@@@@@@ self.exclusionsDict:%s" % self.exclusions)
            else:
                print(" no exclusion rule.")
        else:
            print(" no exclusion setting.")

        # inclusionsDict
        if self.getProperty(self.SETTING_INCLUSIONS) is not None:
            tmp = self.getProperty(self.SETTING_INCLUSIONS)
            print(" inclusionsDict setting:%s" % tmp)
            if tmp is not None:
                self.inclusions = eval(tmp)
                if self.debug:
                    print("@@@@@@@@@@@@@@@@ self.inclusionsDict:%s" % self.inclusions)
            else:
                print(" no inclusion rule.")
        else:
            print(" no inclusion setting.")

            
    #
    # perform server start/stop
    #
    def processRequest(self, **kwargs):
        print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ WatchdogWatcher processRequest: kwarg=%s" % kwargs)

        if kwargs is not None:
            if kwargs.has_key('command'):
                if kwargs['command'] =='start':
                    self.startServer()
                elif kwargs['command']=='stop':
                    self.stopServer()


    #
    # start the server: create an inode observer
    #
    def startServer(self):
        print(" ", time.asctime(), " request server start...")
        self.watching=True
        self.observer = Observer()
        self.observers = []
        self.handlers=[]

        for mission in self.inbox.keys():
            aPath = self.inbox[mission]
            if self.debug:
                print(" ", time.asctime(), " got observer for mission:%s; path:%s" % (mission, aPath))
            handler = MyHandler()
            handler.my_init(mission)
            self.handlers.append(handler)
            # set handler needed vars
            handler.debug=self.debug
            handler.setExclusions(self.exclusions[mission])
            handler.setInclusions(self.inclusions[mission])
            #
            object = self.observer.schedule(handler, path=aPath, recursive=True)
            self.observers.append(object)
            print(" ", time.asctime(), " observer %s started" % mission)


        #
        self.observer.start()

        # poll
        try:
            while self.watching:
                if self.debug:
                    print(" ", time.asctime(), " sleep...")
                time.sleep(5)
                for handler in self.handlers:
                    size, aPath = handler.getFromQueue()
                    if aPath != None:
                        print(" ", time.asctime(), " %s queue size:%s; got file to convert: %s" % (handler.mission, size, aPath))
                        self.processFile(handler.mission, aPath)
        except KeyboardInterrupt:
            if self.debug:
                print(" ", time.asctime(), " KeyboardInterrupt")
                for o in self.observers:
                    # stop observer if interrupted
                    #o.stop()
                    print(" ############# dir(o):%s" % dir(o))
            self.observer.stop()
            if self.debug:
                print(" ", time.asctime(), " KeyboardInterrupt; observer stopped")
            self.watching=False

        self.observer.join()


    #
    # stop the http server
    #
    def stopServer(self):
        print(" ", time.asctime(), " request server stop...")
        self.watching=False

    #
    #
    #
    def processFile(self, mission, path):
        self.counter=self.counter+1
        print(" ", time.asctime(), " will process %s file[%s]: %s" % (self.counter, mission, path))
        if self.worker is not None:
            self.worker.processRequest(path=path, counter=self.counter)
        else:
            raise Exception("%s %s" % (time.asctime(), " worker is None !!"))



#
# the event handler:
# - we are interrested in the creation and modification
#
class MyHandler(PatternMatchingEventHandler):
    #patterns = ["*.xml", "*.lxml", "*.zip"]


    debug=False
    exclusions=[]
    inclusions = []
    mission=None

    #
    def my_init(self, mission):
        #
        self.mission=mission
        self.workqueue = queue.Queue()

    #
    def setExclusions(self, e):
        n = 0
        for rule in e.split('|'):
            print(" add mission %s exclusion rule[%s]:%s" % (self.mission, n, rule))
            aRe = re.compile(rule)
            self.exclusions.append(aRe)
        if self.debug:
            print(" set handler exclusionsDict:%s" % self.exclusions)

    #
    def setInclusions(self, e):
        n = 0
        for rule in e.split('|'):
            print(" add mission %s inclusion rule[%s]:%s" % (self.mission, n, rule))
            aRe = re.compile(rule)
            self.inclusions.append(aRe)
        if self.debug:
            print(" set handler inclusionsDict:%s" % self.inclusions)
    
    #
    def process(self, event):
        if self.debug:
            print(" handler process; exclusionsDict:%s" % self.exclusions)
        """
        event.event_type 
            'modified' | 'created' | 'moved' | 'deleted'
        event.is_directory
            True | False
        event.src_path
            path/to/observed/file
        """
        # test for exclusion and event creation/modification
        if self.debug:
            print("mission:%s" % self.mission, event.src_path, event.event_type)

        if event.event_type=='created':
            match=False

            # look in match inclusionsDict
            for regex in self.inclusions:
                if regex.match(event.src_path):
                    if self.debug:
                        print(" regex inclusionsDict %s match" % regex.pattern)
                    match=True
                    break
            # look if match exclusionsDict
            for regex in self.exclusions:
                if regex.match(event.src_path):
                    if self.debug:
                        print(" regex exnclusions %s match" % regex.pattern)
                    match=False
                    break
            if match:
                print(" added to queue")
                self.AddToQueue(event)

        else:
            if self.debug:
                print(" not interresting event type")

    # PatternMatchingEventHandler event
    def on_modified(self, event):
        if self.debug:
            print(" on_modified")
        self.process(event)

    # PatternMatchingEventHandler event
    def on_created(self, event):
        if self.debug:
            print(" on_created")
        self.process(event)

    # PatternMatchingEventHandler event
    def on_moved(self, event):
        if self.debug:
            print(" on_moved")
        self.process(event)

    # PatternMatchingEventHandler event
    def on_deleted(self, event):
        if self.debug:
            print(" on_deleted")
        self.process(event)

    #
    # add file to be converted in queue
    #
    def AddToQueue(self, event):
        self.workqueue.put(event.src_path)
        print(" event.src_path:%s added to queue" % event.src_path)

    
    #
    # get file path to be converted from queue
    #
    def getFromQueue(self):
        print(" getFromQueue: empty:%s" % self.workqueue.empty())
        if not self.workqueue.empty():
            return self.workqueue.qsize(), self.workqueue.get(False)
        else:
            return 0, None



            
        
