



class model_proba:

    def __init__(self):
        self.name=None
        self.pieces = {}
        self.neededPieces=['1','2','3']
        print(" init model_proba")

    def setName(self, name):
        self.name=name

    def setNeededPiecesType(self, aList):
        self.neededPieces=aList

    def addInContextAsInput(self, c):
       c.inProductModel = self

    #def isComplete(self):
    #    return len(self.pieces)==len(self.neededPieces)

    def checkPiece(self, piece, context):

        # type match
        match = False
        type=None
        for re in self.neededPieces:
            print(" model_oceansat: test re '%s' vs piece '%s'" % (re.pattern, piece))
            if re.match(piece):
                type=re.pattern
                match=True
                print("  model_oceansat: test re '%s' match" % type)

        #if not type in self.neededPieces:
        if not match:
            print(" model_proba errror: can not use piece of type:%s" % type)
            return False
        else:
            # first piece
            if len(self.pieces)==0:
                basename=piece.split('/')[0]
                print(" model_proba basename:%s" % basename)
                self.setName(basename)

            self.pieces[type]=piece
            return True

    def info(self):
        return " model_oceansat; pieces:%s" % (self.pieces)