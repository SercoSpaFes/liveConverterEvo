import time,os, sys, watchdog
import thread
import threading
import inotify.adapters
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
class InotifyWatcher(Service):
    # are dictionnaries: {"mission':'value', ...}
    SETTING_INBOX='INBOX'
    SETTING_EVENTS='INBOX_EVENTS'
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
        self.exclusionsDict = None
        self.inclusionsDict = None
        self.watching=False
        self.workqueue=None
        self.eventsMap={}
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
            print(" InotifyWatcher init:%s" % self.dumpProperty())
        
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

        # events
        if self.getProperty(self.SETTING_EVENTS) is not None:
            if self.debug:
                print("event setting:%s" % self.getProperty(self.SETTING_EVENTS))
            self.eventsMap = eval(self.getProperty(self.SETTING_EVENTS))
            if self.debug:
                print("@@@@@@@@@@@@@@@@ self.eventsMap:%s" % self.eventsMap)


        # exclusionsDict
        if self.getProperty(self.SETTING_EXCLUSIONS) is not None:
            tmp = self.getProperty(self.SETTING_EXCLUSIONS)
            print(" exclusionsDict setting:%s" % tmp)
            if tmp is not None:
                self.exclusionsDict=eval(tmp)
                if self.debug:
                    print("@@@@@@@@@@@@@@@@ self.exclusionsDict:%s" % self.exclusionsDict)
            else:
                print(" no exclusion rule.")
        else:
            print(" no exclusion setting.")

        # inclusionsDict
        if self.getProperty(self.SETTING_INCLUSIONS) is not None:
            tmp = self.getProperty(self.SETTING_INCLUSIONS)
            print(" inclusionsDict setting:%s" % tmp)
            if tmp is not None:
                self.inclusionsDict = eval(tmp)
                if self.debug:
                    print("@@@@@@@@@@@@@@@@ self.inclusionsDict:%s" % self.inclusionsDict)
            else:
                print(" no inclusion rule.")
        else:
            print(" no inclusion setting.")

            
    #
    # perform server start/stop
    #
    def processRequest(self, **kwargs):
        print(" ==> InotifyWatcher processRequest: kwarg=%s" % kwargs)

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
        print(" %s: request server start...", time.asctime())
        self.watching=True
        self.observers = []
        self.handlers={}
        self.observer = inotify.adapters.Inotify()

        for mission in self.inbox.keys():
            aPath = self.inbox[mission]
            if self.debug:
                print(" ", time.asctime(), " got observer for mission:%s; path:%s" % (mission, aPath))
            handler = MyHandler()
            interrestingEvents=None
            if self.eventsMap.has_key(mission):
                interrestingEvents=self.eventsMap[mission]
            if self.debug:
                print(" @@##@@ interrestingEvents:%s" % interrestingEvents)
            handler.my_init(mission, interrestingEvents)
            # set handler needed vars
            handler.debug=self.debug
            handler.setExclusions(self.exclusionsDict[mission])
            handler.setInclusions(self.inclusionsDict[mission])
            self.handlers[aPath]=handler
            #
            self.observer.add_watch(aPath)
            if self.debug:
                print(" ", time.asctime(), " observer %s started" % mission)


        #
        for event in self.observer.event_gen():
            if event is not None:
                (header, type_names, watch_path, filename) = event

                if self.debug:
                    print("WD=(%d) MASK=(%d) COOKIE=(%d) LEN=(%d) MASK->NAMES=%s "
                             "WATCH-PATH=[%s] FILENAME=[%s]",
                             header.wd, header.mask, header.cookie, header.len, type_names,
                             watch_path, filename)

                for aPath in self.handlers.keys():
                    if aPath==watch_path:
                        self.handlers[aPath].process(event)
            else:
                for aPath in self.handlers.keys():
                    size, aDict =  self.handlers[aPath].getFromQueue()
                    if aDict != None:
                        print(" %s: %s queue size:%s; got file to convert: %s" %  (time.asctime(), handler.mission, size, aDict))
                        self.processFile(aDict)
                time.sleep(1)



    #
    # stop the http server
    #
    def stopServer(self):
        print(" ", time.asctime(), " request server stop...")
        self.watching=False

    #
    #
    #
    def processFile(self, aDict):
        self.counter=self.counter+1
        #print(" ", time.asctime(), " will process file[%s]; dict: %s" % (self.counter, aDict))
        if self.worker is not None:
            event = aDict['event']
            mission = aDict['mission']
            regex = aDict['regex']
            allRegex = aDict['allRegex']
            #(header, type_names, watch_path, filename) = event
            self.worker.processRequest(mission=mission, regex=regex, allRegex=allRegex, counter=self.counter, event=event)
        else:
            raise Exception("%s %s" % (time.asctime(), " worker is None !!"))


#
# the event handler:
# - we are interrested in the creation and modification
#
class MyHandler():

    debug = False

    #
    def __init__(self):
        self.exclusions = []
        self.inclusions = []
        self.mission = None

    #
    def my_init(self, mission, eventList):
        #
        self.enventList=eventList
        self.mission = mission
        self.workqueue = queue.Queue()

    #
    def setExclusions(self, e):
        n = 0
        for rule in e.split('|'):
            if self.debug:
                print(" add mission %s exclusion rule[%s]:%s" % (self.mission, n, rule))
            aRe = re.compile(rule)
            self.exclusions.append(aRe)
            n += 1
        if self.debug:
            print(" set handler exclusionsList;num=%s; list:%s" % (len(self.exclusions), self.exclusions))

    #
    def setInclusions(self, e):
        if self.debug:
            print(" $$$$$$ add mission %s inclusion rules:%s" % (self.mission, e))
        n = 0
        for rule in e.split('|'):
            if self.debug:
                print(" add mission %s inclusion rule[%s]:%s" % (self.mission, n, rule))
            aRe = re.compile(rule)
            self.inclusions.append(aRe)
            n+=1
        if self.debug:
            print(" set handler inclusionsList;num=%s; list:%s" % (len(self.inclusions), self.inclusions))

    #
    def process(self, event):
        if self.debug:
            print(" handler process; exclusionsDict:%s" % self.exclusions)

        (header, type_names, watch_path, filename) = event

        # test for exclusion and event creation/modification
        if self.debug:
            print("mission:%s" % self.mission, watch_path, filename, type_names)

        if type_names[0] in self.enventList:
            match = False

            # look in match inclusionsDict
            matchRegex=None
            for regex in self.inclusions:
                if regex.match(filename):
                    if self.debug:
                        print(" regex inclusionsDict %s match" % regex.pattern)
                    match = True
                    matchRegex=regex
                    break
            # look if match exclusionsDict
            for regex in self.exclusions:
                if regex.match(filename):
                    if self.debug:
                        print(" regex exnclusions %s match" % regex.pattern)
                    match = False
                    break
            if match:
                if self.debug:
                    print(" add to queue for mission %s:%s" % (self.mission, event))
                self.AddToQueue(event, self.mission, matchRegex, self.inclusions)

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
    def AddToQueue(self, event, mission, regex, allRegex):
        aDict={}
        aDict['event']=event
        aDict['mission'] = mission
        aDict['regex'] = regex
        aDict['allRegex'] = allRegex
        self.workqueue.put(aDict)
        if self.debug:
            print(" event dict:%s added to queue" % aDict)

    #
    # get file path to be converted from queue
    #
    def getFromQueue(self):
        if self.debug:
            print(" getFromQueue: empty:%s" % self.workqueue.empty())
        if not self.workqueue.empty():
            aDict = self.workqueue.get(False)
            #(header, type_names, watch_path, filename) = self.workqueue.get(False)
            #return self.workqueue.qsize(), "%s/%s" % (watch_path, filename)
            return self.workqueue.qsize(), aDict
        else:
            return 0, None



            
        
