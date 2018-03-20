import urllib
import urllib2

import sys

debug = True

#
# no operation service, skeleton
#
#
#

from service import Service

class ProbaLogic(Service):


    #
    # class init
    # call super class
    #
    def __init__(self, name=None):
        Service.__init__(self, name)


    #
    # init
    # call super class
    #
    # param: p is usually the path of a property file
    #
    def init(self, p=None, worker=None):
        Service.init(self, p, worker)
        self.my_init()


    #
    # init done after the properties are loaded
    # do:
    # - check if DEBUG option set
    #
    def my_init(self):
        # DEBUG setting
        d=self.getProperty(self.SETTING_DEBUG)
        if d is not None:
            self.useDebugConfig(d)
            


    #
    #
    def processRequest(self, **kwargs):
        if self.debug!=0:
            print(" processRequest; kvarg:%s" % (kwargs,))






if __name__ == '__main__':
    propertiePath=None
    if len(sys.argv) > 1:
        propertiePath=sys.argv[1]
        print(" will use property file at path:%s:" % propertiePath)

    noopp = NoopService(name='test noop')
    noopp.setDebug(True)
    noopp.init(propertiePath)

