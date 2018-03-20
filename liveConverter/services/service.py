# -*- coding: cp1252 -*-
#
# this class is a base class for Service
#
#
from abc import ABCMeta, abstractmethod
from cStringIO import StringIO
import os, sys
import logging
import ConfigParser

debug=1
    
class Service:
    #
    SETTING_DEBUG='DEBUG'
    #
    name=None
    propertieFile=None
    properties=None
    worker=None
    ready=True


    #
    # init simple
    # param: p is a string
    #
    def __init__(self, name=None):
        self.name=name
        self.debug=debug
        self.worker=None
        if self.debug!=0:
            print(" create class Service; name=%s" % self.name)

    #
    def useDebugConfig(self, s):
        print(" service %s use debug setting (should be True/False):'%s'; type:%s" % (self.name, s, type(s)))
        if s.lower()=="true":
            self.debug=1
        elif s.lower()=="false":
            self.debug=0
        else:
            raise Exception("invalid debug setting (should be True/False):'%s'" % s)

    #
    def setDebug(self, d):
        if not isinstance( d, int ):
            print("ERROR setDebug: parameter is not an integer")
        print(" Service %s setDebug:%s" %  (self.name, d))
        self.debug=d
        
    #
    def getDebug(self):
        return self.debug

    #
    def setWorker(self, i):
        if self.debug != 0:
            print(" Service setWorker:%s" % (i))
        self.worker = i
        

    #
    # init with worker + property file
    #
    # param: p is usually the path of a property file, can be local to worker './xxx.props' or absolute '/...path.../xxx.props'. Can be None
    #        worker is the worker instance, is used to get the property file path
    #
    # TODO: leave the worker load the ressources
    #
    def init(self, p=None, worker=None):
        self.worker=worker
        if p is not None:
            if self.debug!=0:
                print(" # init class Service with parameter:%s" % (p))
            if p[0:2]=="./":
                p="%s/%s" % (self.worker.getHomeDir(), p[2:])
            if self.debug!=0:
                print(" using service property file at path:'%s'" % p)
            self.propertieFile=p
            self.loadProperties()
        else:
            if self.debug!=0:
                print(" # init class Service with no parameter")



    #
    # load the properties setting file
    #
    def loadProperties(self):
        if self.debug!=0:
            print(" load properties in Service '%s' from file:%s" % (self.name, self.propertieFile))
        __config = ConfigParser.RawConfigParser()
        __config.optionxform=str
        __config.read(self.propertieFile)
        self.properties=dict(__config.items("GLOBAL"))
        
    #
    #
    def getproperties(self):
        return self.properties

    #
    # return a property value
    #
    def getProperty(self, propName=None):
        try:
            return self.properties[propName]
        except:
            return None

    #
    #
    #
    def dumpProperty(self):
        out=StringIO()
        print >>out, 'service properties dump:'
        for key in self.properties.keys():
                print >>out, " key:'%s'; value:'%s'" % (key, self.properties[key])
        return out.getvalue()

    #
    # process a request
    #
    @abstractmethod
    def processRequest(self, **kwargs):
        raise Exception("abstract!!")

    

if __name__ == '__main__':
    print("start")
    logging.basicConfig(level=logging.WARNING)
    log = logging.getLogger('example')
    try:
        s=Service("a=1")
        s.processRequest()
    except:
        log.exception('Error from throws():')

