
DEBUG = True

class BaseFlow:


    #
    STEP_AGREGATE = 'agregate'
    STEP_CONVERT = 'convert'
    STEP_CIRCULATE = 'circulate'

    #
    #
    def __init__(self, name=None):
        self.name = name
        self.debug = DEBUG
        self.steps=[]
        self.conditions = []
        self.failureIsOk = []
        self.currentStepIndex=0
        self.lastDoneStepName=None

        if self.debug != 0:
            print(" create BaseFlow")

    #
    #
    #
    def info(self):
        return "Flow:\n steps:%s\n conditions:%s\n current index:%s; finished?:%s" % (self.steps, self.conditions, self.currentStepIndex, self.isFinished())


    #
    #
    #
    def start(self):
        self.currentStepIndex=0

    #
    #
    #
    def isFinished(self):
        return self.currentStepIndex==len(self.steps)

    #
    #
    #
    def doStep(self, context):
        print(" @@@@ flow test is finished: current:%s; total:%s" % (self.currentStepIndex, len(self.steps)))
        if self.currentStepIndex==len(self.steps):
            return False
        else:
            ok, proceed=self.doStepImpl(context)
            if ok:
                self.currentStepIndex+=1
            return ok, proceed

    #
    #
    #
    def getStepInfo(self):
        return self.steps[self.currentStepIndex]

    #
    #
    #
    def doStepImpl(self, c):
        print(" doStepImpl:%s" % self.steps[self.currentStepIndex])
        return True, True


