import logging

class Logger:
    def __init__(self , name):
        self.name = name
        logging.basicConfig(level=logging.DEBUG)
        self.rootLogger = logging.getLogger(name)
        self.logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
        self.consoleHandler = logging.StreamHandler()
        self.consoleHandler.setFormatter(self.logFormatter)
        self.rootLogger.addHandler(self.consoleHandler)
        self.rootLogger.propagate = False

    def setLogFile(self,logPath,fileName):
        self.fileHandler = logging.FileHandler("{0}/{1}.log".format(logPath, fileName))
        self.fileHandler.setFormatter(self.logFormatter)
        self.rootLogger.addHandler(self.fileHandler)
        self.rootLogger.propagate = False

    def getLogger (self):
        return self.rootLogger

    def debug(self, message):
        self.rootLogger.debug(message) 

    def error(self, message):
        self.rootLogger.error(message) 

    def info(self, message):
        self.rootLogger.info(message) 

    def critical(self, message):
        self.rootLogger.critical(message) 


