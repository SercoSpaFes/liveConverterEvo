




from baseFlow import BaseFlow


#
#
#
class agregateConvertAndCirculateFlow(BaseFlow):

    #
    #
    def __init__(self, name=None):
        BaseFlow.__init__(self, name)
        #
        self.steps=[BaseFlow.STEP_AGREGATE, BaseFlow.STEP_CONVERT, BaseFlow.STEP_CIRCULATE]
        self.conditions=[True, True, True]
        self.failureIsOk = [True, False, False]
        #
        print(" created agregateConvertAndCirculateFlow")




    #
    # return True/False if step ok/failed + True/False if preceed to next step
    #
    def doStepImpl(self, context):

        #
        # TEST conversion step
        #
        if 1 == 2:
            context.setFlowStep(BaseFlow.STEP_CONVERT)
            self.lastDoneStepName = BaseFlow.STEP_CONVERT
            context.inProductModel.convert(context)
            if context.outProduct.isComplete():  # return ok, proceed flags
                return True, True
            else:
                return False, False


        print(" doStepImpl:%s" % self.steps[self.currentStepIndex])
        self.lastDoneStepName=self.steps[self.currentStepIndex]

        #
        # test agregate ok in src product model
        #
        if self.steps[self.currentStepIndex]==BaseFlow.STEP_AGREGATE:
            context.setFlowStep(BaseFlow.STEP_AGREGATE)
            # check if new file (possible product piece) match, also is complete product
            context.inProductModel.checkPiece(context)
            print(" doStepImpl:%s; complete:%s" % (self.steps[self.currentStepIndex], context.inProduct.isComplete()))
            if context.state == "already done":
                return False, False
            else:
                if context.inProduct.isComplete(): # return ok, proceed flags
                    return True, True
                else:
                    return True, False

        #
        #
        #
        elif self.steps[self.currentStepIndex]==BaseFlow.STEP_CONVERT:
            context.setFlowStep(BaseFlow.STEP_CONVERT)
            context.inProductModel.convert(context)
            if context.outProduct.isComplete(): # return ok, proceed flags
                return True, True
            else:
                return False, False

        #
        #
        #
        elif self.steps[self.currentStepIndex]==BaseFlow.STEP_CIRCULATE:
            context.setFlowStep(BaseFlow.STEP_CIRCULATE)
            context.inProductModel.circulate(context)
            if context.circulation.isComplete(): # return ok, proceed flags
                print("FFFFFFFFFFFFFFFFFFFUUUUUUUUUUUUUUUUUUUUUUUUUUCKKKK")
                return True, True
            else:
                print("FFFFFFFFFFFFFFFFFFFUUUUUUUUFFFFFFFFFFFFFFFFFFFUUUUUUUUUUUUUUUUUUUUUUUUUUCKKKKUUUUUUUUUUUUUUUUUUCKKKK")
                return False, False

        else:
            raise Exception("unknown styep:%s" % self.steps[self.currentStepIndex])
