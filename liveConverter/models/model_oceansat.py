from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Boolean, BigInteger
from sqlalchemy.orm.util import has_identity
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship

import zipfile, os, sys, traceback
import ConfigParser
import shutil

#
import datetime
#
Base = declarative_base()

#
CONSTANT_AGREGATED_FOLDER='oceansat_agregated'

#
DEBUG=False



class model_oceansat:

    #
    #
    #
    def __init__(self, context):
        self.ID='OCEANSAT'
        self.debug=DEBUG
        self.neededPieces=['1','2','3','4']
        self.initDbModel(context)
        print(" init model_oceansat")

    #
    #
    #
    def initDbModel(self, context):
        Base.metadata.create_all(context.app.dbAdapter.getEngine())


    #
    #
    #
    def setNeededPiecesType(self, aList):
        self.neededPieces=aList













#
#
#  circulation part
#
#

    #
    # called from agregateConvertAndCirculateFlow when out product is converted well
    #
    def circulate(self, context):
        print(" model_oceansat: will circulate out product '%s'" % (context.outProduct.path))

        try:
            # circulation class and option:
            wClass = None
            wOptions = None
            # setting is like: {'wClass':'None', 'wOptions':'./ressources/circulate_proba.props'}
            tmp = context.app.circulationMap[self.ID]
            print("tmp:%s; type:%s" % (tmp, type(tmp)))
            print("class=%s; option=%s" % (tmp['wClass'], tmp['wOptions']))
            aPackage = tmp['wClass']
            #aClass = aPackage.split('.')[1]
            anOption = tmp['wOptions']

            # read converter options
            config = ConfigParser.RawConfigParser()
            config.optionxform = str
            config.read(anOption)
            properties = dict(config.items("GLOBAL"))
            print("readed properties:%s" % (properties))
            converterConfig = properties.get('configurationFile')
            outSpace = properties.get('OUTSPACE')
            print(" outSpace:%s" % (outSpace))
            if not os.path.exists(outSpace):
                os.makedirs(outSpace)
                print(" outSpace folder created:%s" % (outSpace))

            # test: just copy  in  outspace
            name = os.path.basename(context.outProduct.path)
            destPath = "%s/%s" % (outSpace, name)
            shutil.copy(context.outProduct.path, destPath)

            # update info
            sSize = os.stat(destPath).st_size
            #
            aDate = completedddate = datetime.datetime.utcnow()
            flowStep = OceansatSrcFlowStep(name=context.getFlowStep(), done=True, status='OK', updateddate=aDate, completedddate=aDate)
            rec=context.getDbRecord()
            rec.flowstep.append(flowStep)
            rec.completed=True
            rec.size=sSize
            rec.completedddate = aDate
            context.app.dbAdapter.getSession().commit()

            context.circulation.setComplete(True)
            return True

        except:
            context.circulation.setComplete(True)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            message="Error circulating '%s': %s %s" % (context.outProduct.path, exc_type, exc_obj)
            context.message = message
            return False














#
#
#  convertion part
#
#


    #
    # called from agregateConvertAndCirculateFlow when src product is agregated well
    #
    def convert(self, context):
        self.debug=True
        print(" model_oceansat: TEST convert '%s'" % (context.inProduct.path))
        filename=None
        try:
            exitCode=-1

            # get product key: common name of pieces filename
            filename = os.path.basename(context.inProduct.path)
            name=filename.split('.')[0]
            print(" model_oceansat: will convert '%s' agregated product" % (name))
            context.setFilename(name)

            # get from db
            if not self.existsInDb(context):
                print(" @@##@@ model_oceansat record '%s' NOT in db" % name)
                raise Exception(" oceansat record '%s' NOT found db" % name)
            else:
                print(" @@##@@ model_oceansat record '%s' IN db" % name)
                rec = self.getFromDb(context)
                context.setDbRecord(rec)

                # wrapper class and option:
                wClass=None
                wOptions=None
                # setting is like: 'wClass=JavaEoSipCOnverterWrapper; wOptions=./ressources/JavaEoSipConverterWrapper_proba.props'
                tmp = context.app.conversionMap[self.ID]
                print("tmp:%s; type:%s" % (tmp, type(tmp)))
                print("class=%s; option=%s" % (tmp['wClass'], tmp['wOptions']))
                aPackage = tmp['wClass']
                aClass = aPackage.split('.')[1]
                anOption = tmp['wOptions']

                # read converter options
                config = ConfigParser.RawConfigParser()
                config.optionxform = str
                config.read(anOption)
                properties = dict(config.items("GLOBAL"))
                print("readed properties:%s" % (properties))
                converterConfig = properties.get('configurationFile')
                converterPackage  = properties.get('package')
                doneFlagFile = "%s/%s_doneflagfile.txt"  % (context.app.tmpFolder, rec.name)
                outSpace = "%s/%s_eosips" % (context.app.tmpFolder, self.ID)
                if not os.path.exists(outSpace):
                    os.makedirs(outSpace)
                    print(" create tmp converted product folder:%s" % outSpace)

                if self.debug:
                    print(" wrapper package:%s; options:%s. Converter package:%s; configuration file:%s" % (aPackage, anOption, converterPackage, converterConfig))

                module = __import__(aPackage)
                module2 = getattr(module, aClass)
                class_ = getattr(module2, aClass)
                if self.debug:
                    print(" got wrapper class_:%s" % class_)
                    print(" DIRclass_:%s" % dir(class_))
                #
                aWrapperClass = class_(converterPackage, converterConfig, doneFlagFile, 0)
                if self.debug:
                    print(" got wrapper class:%s" % aWrapperClass)

                # build converter arguments
                params={}
                params['-t'] = context.app.tmpFolder
                params['-i'] = '1'
                params['-c'] = converterConfig
                params['-o'] = outSpace


                print(" will start converter wrapper with params:%s" % params)
                exitCode = aWrapperClass.start(params)
                print("  wrapper exitCode:%s" % exitCode)

                # read from doneFlagFile
                fd=open(doneFlagFile,'r')
                data=fd.read()
                fd.close()
                exitCode2=data.split('\n')[0]
                message=data.split('\n')[1]
                print("  wrapper message:%s" % message)

                # test: just copy zip as out product
                convertedPath="%s/%s_eosip.ZIP" % (outSpace, rec.name)
                shutil.copy(context.inProduct.path, convertedPath)

                # update info
                sSize = os.stat(convertedPath).st_size
                context.outProduct.path = convertedPath
                context.outProduct.setComplete(True)
                #
                aDate=completedddate=datetime.datetime.utcnow()
                flowStep = OceansatSrcFlowStep(name=context.getFlowStep(), done=True,  status=message, updateddate=aDate, completedddate=aDate)
                rec.flowstep.append(flowStep)
                #rec.completed=True
                #rec.completeddate=datetime.datetime.utcnow()
                context.app.dbAdapter.getSession().commit()



        except:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            message="Error converting '%s': %s %s" % (filename, exc_type, exc_obj)
            print(message)
            context.message = message
            traceback.print_exc(file=sys.stdout)
            #
            if context.getDbRecord() is not None:
                aDate = completedddate = datetime.datetime.utcnow()
                flowStep = OceansatSrcFlowStep(name=context.getFlowStep(), status=message, updateddate=aDate)
                context.getDbRecord().flowstep.append(flowStep)
                context.app.dbAdapter.getSession().commit()
            return exitCode











#
#
#  agregation part
#
#


    #
    # called from agregateConvertAndCirculateFlow when new piece arrive
    #
    def checkPiece(self, context):
        # type match: regex pattern match
        piece = context.inProduct.getName()
        match = False
        for re in self.neededPieces:
            print(" model_oceansat: test re '%s' vs piece '%s'" % (re.pattern, piece))
            if re.match(piece):
                context.piecePattern=re.pattern
                match=True
                print("  model_oceansat: test re '%s' match" % context.piecePattern)

        #if not type in self.neededPieces:
        if not match:
            print(" model_oceansat errror: can not use piece of type:%s" % context.piecePattern)
            return False
        else:
            # get product key: common name of pieces filename
            name=piece#.split('/')[-1]
            if name.index('.')>=0:
                name=name[0: name.index('.')]
            if len(name)==0:
                raise Exception("invalid piece path:'%s'" % piece)
            #print(" @@##@@@@##@@@@##@@@@##@@@@##@@@@##@@@@##@@ model_oceansat name:%s" % name)
            context.setFilename(name)
            #print(" @@##@@ model_oceansat name is now:%s" % context.getFilename())
            # TODO: do it once
            self.modelExistsInDb(context)
            print(" @@##@@ model_oceansat recordsNumberInDb:%s" % self.totalInDb(context))

            # persists/update in db
            if not self.existsInDb(context):
                print(" @@##@@ model_oceansat record '%s' NOT in db" % name)
                self.insertInDb(context)
                context.message = "agregate not finished: has 1 piece(s) out of %s" % (len(self.neededPieces))
            else:
                print(" @@##@@ model_oceansat record '%s' IN db" % name)
                rec=self.getFromDb(context)
                context.setDbRecord(rec)
                print(" @@##@@ model_oceansat record '%s' is:%s" % (name, rec))

                #
                if rec.completed:
                    context.message="already done"
                    context.stopFlow = True
                    return True

                if not self.rulePresentInRulesString(context.piecePattern, rec.rules):
                    print(" @@##@@ model_oceansat record '%s' rules will change from '%s'. Will add: '%s'" % (name, rec.rules, context.piecePattern))
                    toks = rec.rules.split('|')
                    toks.append(context.piecePattern)
                    # update rules
                    rec.rules=self.ruleListToSortedRuleString(toks)

                    # add children
                    sSize = os.stat(context.inProduct.path).st_size
                    piece = OceansatSrcPieces(path=context.inProduct.path, rule=context.piecePattern, size=sSize)
                    rec.children.append(piece)
                    # update flow info updateddate
                    n=0
                    currentStep=None
                    for step in rec.flowstep:
                        print(" @@##@@ model_oceansat record '%s' flow step[%s]:%s" % (name, n, step))
                        if step.name==context.getFlowStep():
                            currentStep=step
                            step.updateddate = datetime.datetime.utcnow()

                    # is complete?
                    if len(toks) == len(self.neededPieces):
                        print(" @@##@@ model_oceansat piece '%s' IS COMPLETE 0" % (name))
                        context.inProduct.setComplete(True)
                        context.message = "agregate completed"
                        currentStep.status="OK"
                        self.buildAgregatedProduct(context)
                        print(" @@##@@ model_oceansat inProduct:'%s'" % (context.inProduct.info()))
                    else:
                        context.message="agregate not finished: has %s piece(s) out of %s" % (len(toks), len(self.neededPieces))
                        print(" @@##@@ model_oceansat piece '%s' IS NOT COMPLETE 0:%s" % (name, self.neededPieces))
                    context.app.dbAdapter.getSession().commit()
                    print(" @@##@@ model_oceansat record %s rules updated: %s" % (name, rec))
                else:
                    print(" @@##@@ model_oceansat record '%s' NO rules change: rec rules:'%s' already contains:'%s'" % (name, rec.rules, context.piecePattern))
                    toks = rec.rules.split('|')
                    # is complete?
                    if len(toks) == len(self.neededPieces):
                        print(" @@##@@ model_oceansat piece '%s' IS COMPLETE 1" % (name))
                        # flag inProduct
                        context.inProduct.setComplete(True)
                        self.buildAgregatedProduct(context)
                        context.app.dbAdapter.getSession().commit()
                        context.message = "agregate completed"
                        print(" @@##@@ model_oceansat inProduct:'%s'" % (context.inProduct.info()))
                    else:
                        context.message = "agregate not finished: has %s piece(s) out of %s" % (len(toks), len(self.neededPieces))
                        print(" @@##@@ model_oceansat piece '%s' IS NOT COMPLETE 1:%s" % (name, self.neededPieces))
            return True



    #
    # build the agregated product zip file
    #
    def buildAgregatedProduct(self, context):
        rec = context.getDbRecord()
        agregatedFolder="%s/%s" % (context.app.tmpFolder, CONSTANT_AGREGATED_FOLDER)
        if not os.path.exists(agregatedFolder):
            os.makedirs(agregatedFolder)
            print("  agregated tmp folder created'%s'" % (agregatedFolder))
        destPathPart = "%s/%s.zip_part" % (agregatedFolder, rec.name)
        destPath = "%s/%s.zip" % (agregatedFolder, rec.name)
        if self.debug:
            print("  buildAgregatedProduct: path='%s'" % (destPath))
        zipf = zipfile.ZipFile(destPathPart, mode='w', allowZip64=True)
        for child in context.dbRecord.children:
            if self.debug:
                print("  add to agregated product: %s as %s" % (child.path, os.path.basename(child.path)))
            zipf.write(child.path, os.path.basename(child.path), zipfile.ZIP_DEFLATED)
        zipf.close()

        # rename .zip_part in .zip
        os.rename(destPathPart, destPath)
        print("   agregated product done:%s" % (destPath))
        context.outProduct.path=destPath

        # stat
        sSize = os.stat(destPath).st_size
        rec.path=destPath
        rec.size=sSize

        # update db flow updateddate+done
        n = 0
        for step in rec.flowstep:
            print(" @@##@@ model_oceansat record '%s' flow step[%s]:%s" % (context.getFilename(), n, step))
            if step.name == context.getFlowStep():
                step.updateddate = datetime.datetime.utcnow()
                step.completedddate = datetime.datetime.utcnow()
                step.done=True
                print(" @@##@@ model_oceansat record '%s' flow step[%s]:%s flagged as done" % (context.getFilename(), n, step))

        # update db step updateddate+completed
        n = 0
        for child in rec.children:
            print(" @@##@@ model_oceansat record '%s' child[%s]:%s" % (context.getFilename(), n, child.path))
            child.completedddate = datetime.datetime.utcnow()
            child.completed=True
            print(" @@##@@ model_oceansat record '%s' child[%s]:%s flagged as done" % (context.getFilename(), n, child.path))



    #
    # test if a rule is present in rulesString ('OceansatSrc.rules' in db record)
    #
    def rulePresentInRulesString(self, aPiece, pieceListString):
        toks=pieceListString.split('|')
        for item in toks:
            if aPiece==item:
                return True
        return False

    #
    # build a sorted rulesString, to be put in 'OceansatSrc.rules' db record
    #
    def ruleListToSortedRuleString(self, aList):
        print(" ruleListToSortedRuleString; rules:'%s'" % (aList))
        n=0
        res=''
        for item in sorted(aList):
            print(" ruleListToSortedRuleString[%s]:%s" % (n, item))
            n+=1
            if len(res)>0:
                res+='|'
            res+=item
        return res


    #
    #
    #
    def modelExistsInDb(self, context):
        rec = OceansatSrc()
        if not has_identity(rec):
            print(" @@##@@ model has no identity!!")
        else:
            print(" @@##@@ model has identity!!")
        try:
            self.totalInDb(context)
            print(" @@##@@ model exists in db yes!")
        except:
            print(" @@##@@ no model exists in db yes!")
            self.createModelInDb(context)


    #
    # get OceansatSrc records from db
    #
    def getFromDb(self, context):
        recs = context.app.dbAdapter.getSession().query(OceansatSrc).filter(OceansatSrc.name == context.getFilename()).all()
        if len(recs)>1:
            raise Exception("record %s not unique:%s" % (context.getFilename(), len(recs)))
        return recs[0]


    #
    # insert OceansatSrc in  db
    #
    def insertInDb(self, context):
        rec=OceansatSrc(name=context.getFilename(), rules=context.piecePattern)
        sSize = os.stat(context.inProduct.path).st_size
        piece = OceansatSrcPieces(path=context.inProduct.path, rule=context.piecePattern, size=sSize)
        rec.children.append(piece)
        flowStep = OceansatSrcFlowStep(name=context.getFlowStep())
        rec.flowstep.append(flowStep)
        context.app.dbAdapter.getSession().add(rec)
        context.app.dbAdapter.getSession().commit()
        print(" @@##@@ insertInDb done")
        return rec


    #
    # test if OceansatSrc record exists
    #
    def existsInDb(self, context):
        #print("\n\n\n## existsInDb:")
        #n=0
        #for item in context.app.dbAdapter.getSession().query(db_src_oceansat).filter(db_src_oceansat.name==self.name).all():
        #    print(" match[%s]: %s" % (n, item))
        #    n+=1
        #print("## END existsInDb\n\n\n")

        recs = context.app.dbAdapter.getSession().query(OceansatSrc).filter(OceansatSrc.name == context.getFilename()).all()
        print("existsInDb: rec=%s" % recs)
        if len(recs) >0 :
            return True#, rec
        else:
            return False#, None


    #
    # total of OceansatSrc record in db
    #
    def totalInDb(self, context):
        return len(context.app.dbAdapter.getSession().query(OceansatSrc).all())


    #
    # list OceansatSrc records present in db
    #
    def listInDb(self, context):
        print("\n\n\n## listInDb:")
        n=0
        for item in context.app.dbAdapter.getSession().query(OceansatSrc).all():
            print(" record[%s]: %s" % (n, item))
            n+=1
        print("## END listInDb\n\n\n")


    #
    #
    #
    def info(self):
        return " model_oceansat; pieces:%s" % (self.pieces)


#
# OceansatSrc model
#
class OceansatSrc(Base):
    __tablename__ = 't_oceansat_src'

    id = Column(Integer, primary_key=True)
    name = Column(String(120))
    rules = Column(String(255))
    createddate = Column(DateTime, default=datetime.datetime.utcnow)
    updateddate = Column(DateTime, default=datetime.datetime.utcnow)
    completeddate = Column(DateTime, default=datetime.datetime.utcnow)
    children = relationship("OceansatSrcPieces", back_populates="parent")
    flowstep = relationship("OceansatSrcFlowStep", back_populates="parent")
    path = Column(String(255))
    size = Column(BigInteger)
    completed=Column(Boolean, default=False)
    deleteready = Column(Boolean, default=False)

    def __repr__(self):
        return "<t_oceansat_src(name='%s', rules='%s', completed='%s')>" % (self.name, self.rules, self.completed)


#
# OceansatSrc children (pieces) model
#
class OceansatSrcPieces(Base):
    __tablename__ = 't_oceansat_src_pieces'
    id = Column(Integer, primary_key=True)
    path = Column(String(255))
    rule = Column(String(80))
    createddate = Column(DateTime, default=datetime.datetime.utcnow)
    completedddate = Column(DateTime, nullable=True)
    oceansatsrc_id = Column(Integer, ForeignKey('t_oceansat_src.id'))
    parent = relationship("OceansatSrc", back_populates="children")
    size = Column(BigInteger)
    completed = Column(Boolean, default=False)

    def __repr__(self):
        return "<t_oceansat_src_pieces(path='%s', rule='%s', completed='%s')>" % (self.path, self.rule, self.completed)

#
# OceansatSrc flow step done/completed
#
class OceansatSrcFlowStep(Base):
    __tablename__ = 't_oceansat_src_flow_steps'
    id = Column(Integer, primary_key=True)
    name = Column(String(40))
    oceansatsrc_id = Column(Integer, ForeignKey('t_oceansat_src.id'))
    parent = relationship("OceansatSrc", back_populates="flowstep")
    done = Column(Boolean, default=False)
    status = Column(String(255))
    createddate = Column(DateTime, default=datetime.datetime.utcnow)
    updateddate = Column(DateTime, default=datetime.datetime.utcnow)
    completedddate = Column(DateTime, nullable=True)

    def __repr__(self):
        return "<t_oceansat_src_flow_steps(name='%s', done='%s')>" % (self.name, self.done)