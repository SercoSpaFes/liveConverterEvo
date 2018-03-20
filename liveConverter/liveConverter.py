#
#
#
#
#
#
import os, sys, inspect
import time
import zipfile
import traceback
from cStringIO import StringIO

from baseApp import BaseApp

from esaProducts.live_conversion_inProduct import Live_Conversion_InProduct
from esaProducts.live_conversion_outProduct  import Live_Conversion_OutProduct
from circulation.baseCirculation import BaseCirculation
from converterStatus import ConverterStatus
from context import Context

#
from db.sqlalchemyDbAdapter import sqlalchemyDbAdapter

# mission specific:
from models import model_proba
from models import model_oceansat

#
#
#
class LiveConverter(BaseApp):

    #
    #
    #
    def __init__(self):
        BaseApp.__init__(self)

        self.converterStatus = ConverterStatus()
        self.knownModels={}
        print(" init LiveConverter")


    #
    # process a request comming from inode watcher
    #
    # do the job...
    #
    def processRequest(self, **kwargs):
        startTime = time.time()
        if self.debug:
            print(" processRequest: kwargs=%s" % kwargs)
        mission = kwargs['mission']
        #event=kwargs['event']
        regex = kwargs['regex']
        allRegex = kwargs['allRegex']

        # create a context
        context = self.createContext(kwargs)

        # create a flow
        flow = self.createFlow(mission)
        context.setFlow(flow)


        #
        self.converterStatus.addOneCurrent(context.newFilePath, 'is new')


        proceed=True
        info=None
        while not flow.isFinished() and proceed and not context.stopFlow:
            ok, proceed = flow.doStep(context)
            step = flow.lastDoneStepName

            if context.stopFlow:
                mesg="  -> flow stop; step '%s'; ok:%s; proceed:%s; message:%s" % (step, ok, proceed, context.message)
                print(mesg)
                self.converterStatus.addOneCurrent(context.newFilePath, mesg)
            else:
                mesg = "  -> step '%s'; ok:%s; proceed:%s" % (step, ok, proceed)
                print(mesg)
                if ok:
                    mesg = "  -> step '%s'; ok:%s; proceed:%s; message=%s" % (step, ok, proceed, context.message)
                    print(mesg)
                    self.converterStatus.addOneCurrent(context.newFilePath, mesg)
                else:
                    mesg = "  -> step '%s'; has failed; proceed:%s; message=%s" % (step, proceed, context.message)
                    print(mesg)
                    self.converterStatus.addOneCurrent(context.newFilePath, mesg)


        duration = time.time() - startTime
        if context.stopFlow:
            print(" ==> processRequest stopped at step:%s; duration:%s; message:%s" % (info, duration, context.message))
        else:
            if proceed:
                print(" ==> processRequest COMPLETED: terminated in %s" % duration)
            else:
                print(" ==> processRequest halted at step:%s; duration:%s" % (info, duration))

        print(self.converterStatus.info())




    #
    #
    #
    def createFlow(self, mission):
        if not self.flowsMap.has_key(mission):
            raise Exception('unknown mission:%s' % mission)
        else:
            name = self.flowsMap[mission]
            aPackage = "flows.%s" % name
            if self.debug:
                print(" flow package:%s" % aPackage)
            module = __import__(aPackage)
            module2 = getattr(module, name)
            class_ = getattr(module2, name)
            if self.debug:
                print(" got flow class_:%s" % class_)
                print(" DIRclass_:%s" % dir(class_))
            #
            aclass = class_()
            if self.debug:
                print(" got flow class:%s" % aclass)
            aclass.__init__()
            return aclass

    #
    #
    #
    def createContext(self, kwargs):
        context = Context()
        context.app=self
        #
        event = kwargs['event']
        (header, type_names, watch_path, filename) = event
        mission = kwargs['mission']
        regex = kwargs['regex']
        allRegex = kwargs['allRegex']

        fullPath="%s/%s" % (watch_path, filename)
        context.newFilePath=fullPath

        # create input model if needed:
        aPackage = "models.model_%s" % mission.lower()
        if mission.lower in self.knownModels.keys():
            context.inProductModel=self.knownModels[aPackage]
        else:
            aClass="model_%s" % mission.lower()
            if self.debug:
                print(" mission model package:%s" % aPackage)
            module = __import__(aPackage)
            if self.debug:
                print(" mission model package:%s" % module)
            #print("DIR:%s" % dir(module))
            #
            module2 = getattr(module, aClass)
            if self.debug:
                print(" got class_:%s" % module2)
            #
            class_ = getattr(module2, aClass)
            if self.debug:
                print(" got model class_:%s" % class_)
            #
            aclass = class_(context)
            if self.debug:
                print(" got model class:%s" % aclass)
            aclass.__init__(context)

            self.knownModels[aPackage]=aclass
            aclass.setNeededPiecesType(kwargs['allRegex'])
            context.inProductModel = aclass


        # input product
        context.inProduct = Live_Conversion_InProduct(context.newFilePath)


        # output product
        context.outProduct=Live_Conversion_OutProduct(None)

        # circulation
        context.circulation=BaseCirculation()


        #
        print(context.info())
        return context


#
#
#
if __name__ == '__main__':
    try:
        if len(sys.argv) > 1:
            converter = LiveConverter()
            converter.debug = 1
            exitCode = converter.starts(sys.argv)
            print(" exited with exit code:%s" % exitCode)
            sys.exit(exitCode)
        else:
            print("\nsyntax: python liveConverter.py -c configuration_file.cfg\n")
            sys.exit(1)


    except:
        print(" Error")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        traceback.print_exc(file=sys.stdout)
        sys.exit(2)