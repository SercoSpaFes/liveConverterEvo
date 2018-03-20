




from baseFlow import BaseFlow


#
#
#
class circulate(BaseFlow):

    #
    #
    def __init__(self, name=None):
        BaseFlow.__init__(self, name)
        #
        self.steps = [BaseFlow.STEP_CIRCULATE]
        self.conditions = [True]
        #
        print(" created circulate flow")

    #
    # return True/False if step ok/failed + True/False if preceed to next step
    #
    def doStepImpl(self, context):
        print(" doStepImpl:%s" % self.steps[self.currentStepIndex])
        self.lastDoneStepName = self.steps[self.currentStepIndex]

        #
        #
        #
        if self.steps[self.currentStepIndex] == BaseFlow.STEP_CIRCULATE:
            context.setFlowStep(BaseFlow.STEP_CIRCULATE)
            return True, True

        else:
            raise Exception("unknown styep:%s" % self.steps[self.currentStepIndex])