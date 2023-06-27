from prophecy.config import ConfigBase


class Anotherarray(ConfigBase):
    def __init__(self, prophecy_spark=None, col: str=None, var2: str=None, var3: str=None, var4: str=None, **kwargs):
        self.col = col
        self.var2 = var2
        self.var3 = var3
        self.var4 = var4
        pass


class Arrayofrec(ConfigBase):
    def __init__(self, prophecy_spark=None, val1: str=None, anotherarray: list=None, **kwargs):
        self.val1 = val1
        self.anotherarray = self.get_config_object(prophecy_spark, [], anotherarray, Anotherarray)
        pass


class SubgraphConfig(ConfigBase):

    def __init__(self, prophecy_spark=None, arrayofrec: list=None, **kwargs):
        self.arrayofrec = self.get_config_object(
            prophecy_spark, 
            [Arrayofrec(
               prophecy_spark = prophecy_spark, 
               val1 = "test", 
               anotherarray = [Anotherarray(
                  prophecy_spark = prophecy_spark, 
                  col = "tests", 
                  var2 = "jsd kjhdsf ", 
                  var3 = "sdfsdf", 
                  var4 = "test12"
                )]
             )], 
            arrayofrec, 
            Arrayofrec
        )
        pass

    def update(self, updated_config):
        self.arrayofrec = updated_config.arrayofrec
        pass

Config = SubgraphConfig()
