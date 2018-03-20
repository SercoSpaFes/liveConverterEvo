#
# copyright Serco
# Lavaux Gilles 2017/10
#
# Wrapper for the EoSip converter. 
# Help to use the converter from within Luigi scenarios
#
# Requirements:
#  - PYTHON_PATH has to point to two libraries: the eoSip_converter package + the needed_sip_spec_version package.
#
# syntax: EoSipConverterWrapper.py EoSipConverterInstanceName path_to_converter_configuration path_to_done_file_flag [any eoSip_converter parameter pair][...]
#
#
# V:0.8
# Code quality: RC_1
#

import os, time
import sys
import traceback
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO
from datetime import datetime, timedelta



from sysim import sysItem

#
VERSION='  Lavaux Gilles/Serco 2017. V:0.5'

# constants
BAD_CONVERION_LOG_NAME='bad_conversion_1.log'
ERROR_TB_DELIM='Traceback (most recent call last):'
#

#
DEFAULT_DATE_PATTERN="%Y%m%dT%H%M%S"
REPORT_DATE_PATTERN="%a %Y/%m/%d %H:%M:%S"

#
DEBUG=True


#
# get a file ctime
#
def getFileCtime(aPath, pattern=DEFAULT_DATE_PATTERN):
    ctime = os.path.getctime(aPath)
    dt = datetime.fromtimestamp(ctime)
    return dt.strftime(pattern)

#
#
#
class EoSipConverterWrapper():
    converterInstance=None
    converterWorkPath = None
    doneFlagPath=None
    debug=DEBUG
    
    #
    # 
    #
    def __init__(self, converterName, converterConfigPath, doneFlagPath, startTime):
        print("EoSipConverterWrapper init")
        print VERSION
        self.converterName=converterName
        self.converterInstance=None
        self.converterConfigPath = converterConfigPath
        self.doneFlagPath=doneFlagPath
        self.startTime=startTime

        if not os.path.exists(self.converterConfigPath):
            raise Exception("configuration file does not exists:%s" % self.converterConfigPath)

        # create converter
	    # import eoSip_converter.ingester_xxx
        if self.debug:
            print(" will import package:'%s'" % self.converterName)
        module = __import__(self.converterName)
        print(" converter package imported:%s" % module)

        aName=self.converterName.split('.')[1]
        if self.debug:
            print(" will import class:'%s'" % aName)
        module2 = getattr(module, aName)
        print(" converter package imported:%s" % module2)

        # instanciate class
        class_ = getattr(module2, aName)
        print(" converter class imported:%s" % class_)
        
        self.converterInstance = class_()
        print(" got converter instance:%s" % self.converterInstance)








    # the known type of info we can retrieve from converter logs
    knwonProcessingInfo=['error','eosip','sysitems', 'ctime']
    #
    # return a processing info, readed from various logs
    #
    def getProsessingInfo(self, params, type, converterWorkFolder, summaryFile, context=None):
        if self.debug:
            print("  getProsessingInfo for:'%s'" % type)
        strBuffer = StringIO()
        try:
            #
            # get source and destination
            #
            if type=='eosip':
                eosip=''
                fd=open(summaryFile, 'r')
                data=fd.read()
                fd.close()
                #
                pos = data.find('eosip[0]:')
                if pos > 0:
                    pos2 = data.find('\n', pos)
                    eosip=data[pos+len('eosip[0]:'):pos2]
                #
                src=''
                pos = data.find('done[0]:')
                if pos > 0:
                    pos2 = data.find('\n', pos)
                    src=data[pos+len('done[0]:'):pos2].split('|')[0]
                return src, eosip
            #
            # get source and destination
            #
            if type == 'sysitems':
                eosip = ''
                aPath="./log/sysImgs/%s.img" % context
                fd=open(aPath, 'r')
                data=fd.read()
                fd.close()
                print("\n\n\nSYSITEM:\n%s\n\n" % data)
                lines=data.split('\n')
                srcItem = sysItem.SysItem()
                srcItem.fromString(lines[4])
                eoSipItem = sysItem.SysItem()
                eoSipItem.fromString(lines[5])
                hashSrc = srcItem.getHash()
                hashEoSip = eoSipItem.getHash()
                dateCtime = getFileCtime(aPath, REPORT_DATE_PATTERN)
                print("hashSrc:%s; hashEoSip:%s; ctime:%s" % (hashSrc, hashEoSip, dateCtime))
                return hashSrc, hashEoSip, dateCtime

            #
            # get last line of error file
            #
            elif type=='error':
                aPath = "%s/%s" % (converterWorkFolder, BAD_CONVERION_LOG_NAME)
                fd=open(aPath, 'r')
                data=fd.read()
                fd.close()
                #
                if self.debug:
                    print("\nreaded:%s\n" % data)
                #
                lastLine=''
                done=False
                while not done and len(lastLine.strip())==0:
                    pos = data.rfind('\n')
                    if self.debug:
                        print(" pos=%s" % pos)
                    if pos >=0:
                        lastLine=data[pos:].strip()
                        if self.debug:
                            print(" lastline='%s' " % lastLine)
                        data=data[0:pos-1]
                    else:
                        done=True
                strBuffer.write(lastLine)
            #
            #
            #
            else:
                return "unknown conversion result info type:%s" % type

            #
            print("  getProsessingInfo for:%s returns:'%s'" % (type, strBuffer.getvalue()))
            return strBuffer.getvalue()
        except:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print("Error getting prosessing info '%s': %s %s" % (type, exc_type, exc_obj))
            traceback.print_exc(file=sys.stdout)
            return "Error getting prosessing info '%s': %s %s" % (type, exc_type, exc_obj)







    #
    # build the doneFlagFile
    # put conversion info inside
    #
    def buildDoneFile(self, params, exitCode, fatalMesg):
        a, b = os.path.split(self.doneFlagPath)
        if not os.path.exists(a):
            os.makedirs(a)
            print("  doneFileFlag dir created:%s" % a)
        fd = open(self.doneFlagPath, 'w')
        fd.write("exitCode:%s" % exitCode)

        if fatalMesg is not None:
            fd.write("\nerror:%s\n" % fatalMesg)
        else:
            src='NA'
            if params.has_key('-s'):
                src = params['-s']

            # get conversion info from work folder
            # look for the conversion folder inside self.converterWorkPath
            # folder syntax is like: batch_irs1c1d_luigi_0_workfolder_0
            # 'batch_' + insideConfigFileValue + '_'  + $batchname + '_' + $id + '_workfolder_0'
            #

            #
            # completly fail: no work folder
            #
            if not os.path.exists(self.converterWorkPath):
                fd.write("\nNo conversion working folder found: that's very bad...")
                fd.write("\nsource:%s" % src)
                fd.write("\nat:%s" % (time.time() - float(self.startTime)))
                fd.write("\ntime:%s" % time.time())

            else:
                converterWorkFolder = None
                files = os.listdir(self.converterWorkPath)
                n = 0
                for item in files:
                    if self.debug:
                        print(" fodler %s in %s: %s" % (n, self.converterWorkPath, item))
                    if item.startswith('batch_') and item.endswith('_workfolder_0'):
                        converterWorkFolder = "%s/%s" % (self.converterWorkPath, item)
                        break
                    n += 1
                if converterWorkFolder == None:
                    raise Exception("can not find converter batch fodler inside:%s" % self.converterWorkPath)

                #
                # get info from log folder
                # substitute in basename of converterWorkFolder: '_workfolder_0' with: xxx
                # like: 'batch_irs1c1d_luigi_0_log.txt'
                #
                logBaseName = os.path.basename(converterWorkFolder)
                summaryFile = "./log/%s" % logBaseName.replace('_workfolder_0','_log.txt')
                if self.debug:
                    print(" conversion summary file:%s" % summaryFile)

                #
                if exitCode != 0:
                    # retrieve error
                    error = self.getProsessingInfo(params, 'error', converterWorkFolder, summaryFile)
                    print("  doneFileFlag content added: %s=%s" % ('error', error))
                    fd.write("\n%s" % error)
                    fd.write("\nsource:%s" % src)
                    fd.write("\nat:%s" % (time.time() - float(self.startTime)))
                    fd.write("\ntime:%s" % time.time())
                else:
                    # retrieve info
                    # source and destination paths
                    src, eosip = self.getProsessingInfo(params, 'eosip', converterWorkFolder, summaryFile)
                    fd.write("\nsource:%s" % src)
                    print("  doneFileFlag content added: %s=%s" % ('source', src))
                    fd.write("\neosip:%s" % eosip)
                    print("  doneFileFlag content added: %s=%s" % ('eosip', eosip))
                    #
                    srcMd5, destMd5, ctime= self.getProsessingInfo(params, 'sysitems', converterWorkFolder, summaryFile, os.path.basename(eosip))
                    fd.write("\nsourceMd5:%s" % srcMd5)
                    print("  doneFileFlag content added: %s=%s" % ('srcMd5', srcMd5))
                    fd.write("\neosipMd5:%s" % destMd5)
                    print("  doneFileFlag content added: %s=%s" % ('eosipMd5', destMd5))
                    fd.write("\nctime:%s" % ctime)
                    print("  doneFileFlag content added: %s=%s" % ('ctime', ctime))
                    fd.write("\nat:%s" % (time.time() - float(self.startTime)))
                    fd.write("\ntime:%s" % time.time())
                fd.write("\n")

        fd.flush()
        fd.close()
        print("  doneFileFlag file created:%s" % self.doneFlagPath)



    #
    # launch the converter
	# create the done_flag_file that will be used by Luigi task as output
    # it will contains a summary of the conversion containing pair values (':' separated), that can be used by Luigi task to build report:
    # - exitcode:a code
    # - error: a string
    # - eosip_path: a path
    # - sysitem_x: a sysitem line (usually one line for parent product and one for eosip)
    #
    #
    def start(self, args):
        print("EoSipConverterWrapper.start()")
        # require the -t (temporary folder path) parameter
        # to be able to get conversion info after completion
        aPath=None
        print( " start args:%s" % args)
        #
        if args.has_key('-t'):
            aPath=args['-t']
        else:
            errorMesg = "FATAL: no -t argument given"
            print(" will exit because of " + errorMesg)
            # to be removed: 1 line
            #self.converterWorkPath = './tmp_batch_1/work/0'
            self.buildDoneFile(None, -255, errorMesg)
            return  -255

        #
        self.converterWorkPath=aPath

        # start converter
        params=[]
        params.append("-c")
        params.append(self.converterConfigPath)
        #
        for item in args.keys():
            params.append(item)
            params.append(args[item])

        # report test:
        #self.buildDoneFile(params, 0, None)
        #os._exit(-1)

        print(" starting converter with params:%s" % params)

        # TEST: fail 3rd conversion for test
        id=-1
        if args.has_key('-i'):
            id = int(args['-i'])
            print("############################################################################# id='%s'" % id)
        if id==3:
            exitCode = -55
        else:
            # TO BE REMOVED LATER
            exitCode=0
            #exitCode = self.converterInstance.starts(params)
        print("  converter exit code:%s" % exitCode)
        try:
            # TO BE REMOVED LATER. issue a fatal error
            #self.buildDoneFile(args, exitCode, None)
            self.buildDoneFile(args, exitCode, 'Conversion disabled..')
        except:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(" write doneFlagFile error:%s %s" % (exc_type, exc_obj))
            traceback.print_exc(file=sys.stdout)
        return exitCode

#
# parse parameters needed by eoSip_converter
# at this time: just a few
#
knwonParamsName=['-i', '-l', '-b', '-o', '-t', '-s', '-m']
def parseParams(args):
    aDict={}
    n = len(args)
    if n==0:
        return
    if n%2 !=0:
        i=0
        for item in args:
            print(" param[%s]:%s" % (i, item))
            i+=1
        raise Exception("additionnal parameters have to be in pair: num params givem=%s" % n)
    for n in range(n/2):
        if DEBUG:
            print(" - do params: %s %s" % (args[n*2], args[n*2+1]))
        try:
            knwonParamsName.index(args[n*2])
            aDict[args[n*2]]=args[n*2+1]
        except:
            print("  unknown param!")
            raise Exception("unknown param in anyParams list:%s" % args[n*2])
    return aDict
        

#
# main
#
if __name__ == "__main__":
    try:
        if len(sys.argv) >= 3:
            print("EoSipConverterWrapper converter:%s\n" % sys.argv[1])
            converterInstance = sys.argv[1]
            converterConfigPath = sys.argv[2]
            doneFlagFile = sys.argv[3]
            startTime = sys.argv[4]
            if doneFlagFile.startswith('-'):
                raise Exception("invalid param doneFlagFile:%s" % doneFlagFile)
            anyParam=[]
            if len(sys.argv) > 4:
                anyParam = sys.argv[5:]
            print(" Will start converter:%s using configuration:%s; doneFlagFile:%s and anyParam:%s" % (converterInstance, converterConfigPath, doneFlagFile, anyParam))
            aDict = parseParams(anyParam)
            EoSipConverterWrapper(converterInstance, converterConfigPath, doneFlagFile, startTime).start(aDict)
        else:
            print("syntax: EoSipConverterWrapper.py EoSipConverterInstanceName path_to_converter_configuration path_to_done_file_flag startTime_or_0 [any eosip suppoted parameter]\n")

    except:
        print(" init error:\n")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

