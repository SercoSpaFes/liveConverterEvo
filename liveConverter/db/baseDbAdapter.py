


DEBUG = False

class BaseDbAdapter:

    #
    #
    def __init__(self, name=None, url=None):
        self.name = name
        self.debug = DEBUG
        self.url = url
        if self.debug != 0:
            print(" create BaseDbAdapter")


    #
    #
    #
    def productSchemaExists(self, product):
        return False

    #
    #
    #
    def createProductSchema(self, product):
        return False


    #
    #
    #
    def createProductSchema(self, product):
        return False

