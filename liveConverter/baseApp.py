#
#
#
from abc import ABCMeta, abstractmethod
import os,sys,traceback,inspect
import ConfigParser
import logging
from cStringIO import StringIO
from logging.handlers import RotatingFileHandler
import time,datetime


from services import serviceProvider
from db.sqlalchemyDbAdapter import sqlalchemyDbAdapter

#
#
# sections name in configuration file
SETTING_Main='Main'
SETTING_Watchers='watchers'
SETTING_Logic='logic'
SETTING_Flows='flows'
SETTING_Database='database'
SETTING_Conversion='conversion'
SETTING_Circulation='circulation'
SETTING_Dissemination='dissemination'

#
SETTING_FlowsMap='flowsMap'
SETTING_ConversionMap='conversionMap'
SETTING_CirculationMap='circulationMap'
SETTING_Services='Services'
#
SETTING_dbUrl='dbUrl'



# config name and version
CONFIG_NAME=None
CONFIG_VERSION=None

# folders stuff
SETTING_CONFIG_NAME='CONFIG_NAME'
SETTING_CONFIG_VERSION='CONFIG_VERSION'
SETTING_TMP_FOLDER='CONFIG_TMP_FOLDER'

#
VERSION_INFO="live converter"
VERSION="base live converter V:0.0.0"


#
# fixed stuff
APP_LOG_FILE_NAME='live_converter'
APP_LOG_FILE_EXT='log'
LOG_FOLDER='log'


#
homeDir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))


#
DEBUG = False

#
#
#
class BaseApp():

    #
    #
    #
    def __init__(self):
        print(" init BaseApp")
        debug = DEBUG
        #
        self.serviceProvider=None
        self.usedConfigFile=None
        #
        self.flowsMap={}
        self.conversionMap={}
        self.circulationMap = {}
        #
        self.dbUrl=None
        self.dbAdapter=None

        #
        self.tmpFolder = None
        # logger
        self.LOG_FOLDER = LOG_FOLDER
        self.logger = logging.getLogger()
        # self.logger.setLevel(logging.DEBUG)
        self.logger.setLevel(logging.NOTSET)
        basicFormat = '%(asctime)s - [%(levelname)s] : %(message)s'
        self.formatter = logging.Formatter(basicFormat)
        self.file_handler = RotatingFileHandler("%s.%s" % (APP_LOG_FILE_NAME, APP_LOG_FILE_EXT), '', 1000000, 1)
        self.file_handler.setLevel(logging.DEBUG)
        self.file_handler.setFormatter(self.formatter)
        self.logger.addHandler(self.file_handler)
        steam_handler = logging.StreamHandler()
        steam_handler.setLevel(logging.DEBUG)
        steam_handler.setFormatter(self.formatter)
        self.logger.addHandler(steam_handler)
        #
        self.daemon=False


    #
    #
    #
    def starts(self, args):
        global LOG_FOLDER
        #
        self.args = args
        if DEBUG:
            n = 0
            for item in self.args:
                print(" args[%d]='%s'" % (n, item))
                n += 1

        print("\nstarting %s" % self.getVersion())

        # parse command line parameters
        from optparse import OptionParser
        parser = OptionParser()
        parser.add_option("-c", "--config", dest="configFile", help="path of the configuration file")
        parser.add_option("--daemonClass", dest="daemonClass", help="daemon server classe")
        self.options, args = parser.parse_args(args)

        #
        if self.options.configFile is not None:
            print("\n options readed:\n configuration file:%s\n" % self.options.configFile)
        else:
            raise Exception("need at least a configuration file path as argument")

        if self.options.daemonClass:
            self.daemon=True
            self.daemonClass = self.options.daemonClass
            print(" ==>  daemon classe:%s" % self.daemonClass)
            self.logger.info(" ==> daemon classe:%s" % self.daemonClass)

        # read the config
        self.readConfig(self.options.configFile)
        print("\n\n\n\n configuration readed\n\n")

        # get a db session
        self.dbAdapter.createEngine()
        tmp = self.dbAdapter.getSession()
        print(" db adapter session:%s" % tmp)

        #
        # if we start in daemon mode:
        # - processing will be triggered by the daemonClass in use
        #
        if self.daemon:
            print(" ==> run in daemon mode")
            self.logger.info(" ==> run in daemon mode")
            if self.daemonClass is not None:
                print("   stating daemon classe:%s" % self.daemonClass)
                self.logger.info("   stating daemon classe:%s" % self.daemonClass)

                self.daemonService = self.getService(self.daemonClass)
                print("   daemonService:%s" % self.daemonService)
                self.daemonService.setWorker(self)
                self.daemonService.processRequest(command='start')
            else:
                print("   no daemon class submitted: converter will probably be used by pyro")


    #
    #
    @abstractmethod
    def __wireServices(self):
        result=None
        aMethodName = 'wireService'
        if hasattr(self, aMethodName):
            if self.debug:
                print(" @@@@  wireService function exists")
            meth = getattr(self, aMethodName, None)
            if callable(meth):
                result = meth()
        else:
            if self.debug:
                print(" @@@@  no wireService function")
        return result



    #
    # return derived class VERSION if exists, or this VERSION
    #
    def getVersion(self):
        result=None
        aMethodName = 'getVersionImpl'
        if hasattr(self, aMethodName):
            meth = getattr(self, aMethodName, None)
            if callable(meth):
                result = meth()
        else:
            return VERSION
        return result


    #
    #
    #
    def getHomeDir(self):
        return homeDir


    #
    #
    #
    def readConfig(self, path=None):
        if not os.path.exists(path):
            raise Exception("configuration file:'%s' doesn't exists" % path)

        self.usedConfigFile =path
        self.logger.info("\n\n\n\n\n reading configuration '%s'" % path)
        self.__config = ConfigParser.RawConfigParser()
        self.__config.optionxform =str
        self.__config.read(path)
        #
        self.CONFIG_NAME = self.__config.get(SETTING_Main, SETTING_CONFIG_NAME)
        self.CONFIG_VERSION = self.__config.get(SETTING_Main, SETTING_CONFIG_VERSION)

        # services
        self.readServicesConfiguration()

        # flows
        tmp = self.__config.get(SETTING_Flows, SETTING_FlowsMap)
        if tmp is not None:
            self.flowsMap=eval(tmp)
            print(" @@##@@ flows map:%s" % self.flowsMap)

        # tmp folder
        tmp = self.__config.get(SETTING_Main, SETTING_TMP_FOLDER)
        self.tmpFolder=tmp
        if not os.path.exists(tmp):
            os.makedirs(self.tmpFolder)
            print(" @@##@@ created tmp folder:%s" % self.tmpFolder)

        # DB
        tmp = self.__config.get(SETTING_Database, SETTING_dbUrl)
        if tmp is not None:
            self.dbUrl = tmp
            print(" @@##@@ db Url:%s" % self.dbUrl)
            self.dbAdapter=sqlalchemyDbAdapter('default', self.dbUrl)
        else:
            raise Exception("no dbUrl configured !")

        # conversions
        tmp = self.__config.get(SETTING_Conversion, SETTING_ConversionMap)
        if tmp is not None:
            self.conversionMap=eval(tmp)
            print(" @@##@@ conversion map:%s" % self.conversionMap)

        # circulations
        tmp = self.__config.get(SETTING_Circulation, SETTING_CirculationMap)
        if tmp is not None:
            self.circulationMap=eval(tmp)
            print(" @@##@@ circulation map:%s" % self.circulationMap)

    #
    #
    #
    def readServicesConfiguration(self):
        # servicesProvider: optional
        try:
            serviceProvidersSrc = dict(self.__config.items(SETTING_Services))
            if len(serviceProvidersSrc) != 0:
                self.servicesProvider = serviceProvider.ServiceProvider(None)
                n = 0
                for item in serviceProvidersSrc:
                    try:
                        value = serviceProvidersSrc[item]
                        if self.debug != 0:
                            print(" service[%d]:%s==>%s" % (n, item, value))
                        self.servicesProvider.addService(item, value, self)
                    except:  # fatal
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        print(" Error adding serviceProvider '%s': %s %s\n%s" % (exc_type, exc_obj, item, traceback.format_exc()))
                        os._exit(-52)
                    n = n + 1
            else:
                print(" no service provider configured")
        except:  # no service section
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(" warning servicesProvider:%s %s" % (exc_type, exc_obj))
            if self.debug:
                traceback.print_exc(file=sys.stdout)


    #
    # return the service provider
    #
    def getServiceProvider(self):
        return self.servicesProvider


    #
    # return a service by name
    #
    def getService(self, name):
        if self.servicesProvider == None:
            raise Exception("no service available")

        return self.servicesProvider.getService(name)