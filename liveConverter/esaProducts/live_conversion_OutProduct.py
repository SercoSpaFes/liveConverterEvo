

import os, sys
import re
import logging
import zipfile


from product import Product


class Live_Conversion_OutProduct(Product):

    #
    #
    #
    def __init__(self, path):
        Product.__init__(self, path)
        self.complete = False
        print(" init class Live_Conversion_OutProduct")

    #
    #
    #
    def isComplete(self):
        print("%%%%%%%%%%%%%%%%%%%%%% Live_Conversion_OutProduct: isComplete:%s" % self.complete)
        return self.complete

    #
    #
    #
    def setComplete(self, b):
        self.complete = b
        print("%%%%%%%%%%%%%%%%%%%%%% Live_Conversion_OutProduct: setComplete:%s" % self.complete)

    #
    #
    #
    def info(self):
        return "%%%%%%%%%%%%%%%%%%%%%% Live_Conversion_OutProduct: info:%s" % self.isComplete()