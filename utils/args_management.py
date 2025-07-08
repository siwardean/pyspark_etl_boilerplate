import argparse

class AppArgsManagement:
    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--className', required=True)
        parser.add_argument('--classPackage', required=True)
        parser.add_argument('--confPath', required=True)
        args = parser.parse_args()
        self.className = args.className
        self.classPackage = args.classPackage
        self.confPath = args.confPath

    def getClassName(self): return self.className
    def getClassPackage(self): return self.classPackage
    def getConfPath(self): return self.confPath
