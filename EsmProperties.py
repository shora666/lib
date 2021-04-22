import configparser

class EsmProperties:

        def __init__(self , name):
                self.name = name

        def set_properties_file (self,file):
                self.file=file
                self.config = configparser.ConfigParser()
                self.config.read(self.file)
                self.section = self.config.sections()

        def get_property (self,section,property):
                return self.config.get(section, property, fallback='No such property')
        def get_properties (self,section):
                return self.config.items(section,raw=False, vars=None)
