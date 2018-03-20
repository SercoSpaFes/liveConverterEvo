

class BaseCirculation():

    def __init__(self):
        self.complete = False
        print(" created BaseCirculation")

    #
    #
    #
    def isComplete(self):
        print("%%%%%%%%%%%%%%%%%%%%%% BaseCirculation: isComplete:%s" % self.complete)
        return self.complete

    #
    #
    #
    def setComplete(self, b):
        self.complete = b
        print("%%%%%%%%%%%%%%%%%%%%%% BaseCirculation: setComplete:%s" % self.complete)

    #
    #
    #
    def info(self):
        return "%%%%%%%%%%%%%%%%%%%%%% BaseCirculation: info:%s" % self.isComplete()