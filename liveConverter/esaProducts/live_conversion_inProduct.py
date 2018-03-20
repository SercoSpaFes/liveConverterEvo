

import os, sys
import re
import logging
import zipfile


from product_directory import Product_Directory


class Live_Conversion_InProduct(Product_Directory):

    #
    #
    #
    def __init__(self, path):
        Product_Directory.__init__(self, path)
        self.complete = False
        print(" init class Live_Conversion_InProduct")

    #
    #
    #
    def isComplete(self):
        print("%%%%%%%%%%%%%%%%%%%%%% Live_Conversion_InProduct: isComplete:%s" % self.complete)
        return self.complete

    #
    #
    #
    def setComplete(self, b):
        self.complete = b
        print("%%%%%%%%%%%%%%%%%%%%%% Live_Conversion_InProduct: setComplete:%s" % self.complete)

    #
    #
    #
    def info(self):
        return "%%%%%%%%%%%%%%%%%%%%%% Live_Conversion_InProduct: info:%s" % self.isComplete()

