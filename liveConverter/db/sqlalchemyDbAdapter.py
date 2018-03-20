import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker

from baseDbAdapter import BaseDbAdapter


#
#
#
class sqlalchemyDbAdapter(BaseDbAdapter):

    #
    #
    def __init__(self, name=None, url=None):
        BaseDbAdapter.__init__(self, name, url)
        self.engine = None
        self.sessionClass = None
        self.session=None
        if self.debug != 0:
            print(" create sqlalchemyDbAdapter; name:%s; url:%s" % (self.name, self.url))

        # keep track of controled model schema in db:
        self.knownModels={}


    #
    #
    #
    def createEngine(self):
        self.engine = create_engine(self.url) #, echo=True)

    #
    #
    #
    def getEngine(self):
        return self.engine

    #
    #
    #
    def getSession(self):
        if self.sessionClass is None:
            self.sessionClass=sessionmaker(bind=self.engine)
            #print("@@##@@##@@##@@##@@##@@##@@##@@##@@##@@##@@##@@## sqlalchemyDbAdapter; self.sessionClass:%s" % (self.sessionClass))
            self.session = self.sessionClass()
            #print("@@##@@##@@##@@## sqlalchemyDbAdapter; session created:%s" % (self.session))
        return self.session


