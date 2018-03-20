

#
#
#
class ConverterStatus:

    #
    #
    #
    def __init__(self):
        print(" create ConverterStatus")
        self.productDone = {}
        self.productOK = {}
        self.productFail = {}
        self.currentProducts = {}

    #
    #
    #
    def info(self):
        a = "\n\n ==> converterStatus:\n total products done:%s\n ok:%s\n failed:%s\n current:%s\n\n" % (len(self.productDone), len(self.productOK), len(self.productFail), len(self.currentProducts))
        if len(self.currentProducts)>0:
            a+="  current list:\n"
            n=0
            for key in self.currentProducts.keys():
                a+="\n    %s: %s: %s" % (n, key, self.currentProducts[key])
                n+=1
        return a
    #
    #
    #
    def addOneOK(self, p):
        self.productOK[p.name]=p

    #
    #
    #
    def addOneOFailed(self, p):
        self.productFail[p.name] = p

    #
    #
    #
    def addOneDone(self, p):
        self.productDone[p.name] = p

    #
    #
    #
    def addOneCurrent(self, path, p=None):
        self.currentProducts[path]=p