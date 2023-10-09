from p1.graph.sg2.config.Config import SubgraphConfig as sg2_Config
from prophecy.config import ConfigBase


class Arrayinsiderec(ConfigBase):
    def __init__(self, prophecy_spark=None, val1: str=None, val2: str=None, val3: str=None, **kwargs):
        self.val1 = val1
        self.val2 = val2
        self.val3 = val3
        pass


class Pipearray_of_rec(ConfigBase):
    def __init__(self, prophecy_spark=None, cond: str=None, arrayinsiderec: list=None, **kwargs):
        self.cond = cond
        self.arrayinsiderec = self.get_config_object(prophecy_spark, [], arrayinsiderec, Arrayinsiderec)
        pass


class Arrayin(ConfigBase):
    def __init__(self, prophecy_spark=None, v1: str=None, **kwargs):
        self.v1 = v1
        pass


class Pp(ConfigBase):
    def __init__(self, prophecy_spark=None, cond: str=None, arrayin: list=None, **kwargs):
        self.cond = cond
        self.arrayin = self.get_config_object(prophecy_spark, [], arrayin, Arrayin)
        pass


class Config(ConfigBase):

    def __init__(self, sg2: dict=None, pipearray_of_rec: list=None, pp: list=None, **kwargs):
        self.spark = None
        self.update(sg2, pipearray_of_rec, pp)

    def update(self, sg2: dict={}, pipearray_of_rec: list=None, pp: list=None, **kwargs):
        prophecy_spark = self.spark
        self.sg2 = self.get_config_object(prophecy_spark, sg2_Config(prophecy_spark = prophecy_spark), sg2, sg2_Config)
        self.pipearray_of_rec = self.get_config_object(
            prophecy_spark, 
            [Pipearray_of_rec(
               prophecy_spark = prophecy_spark, 
               cond = "wwer", 
               arrayinsiderec = [Arrayinsiderec(prophecy_spark = prophecy_spark, val1 = "ewsdf", val2 = "sdfsdf", val3 = "fsdfsdf")]
             )], 
            pipearray_of_rec, 
            Pipearray_of_rec
        )
        self.pp = self.get_config_object(
            prophecy_spark, 
            [Pp(
               prophecy_spark = prophecy_spark, 
               cond = "dddd", 
               arrayin = [Arrayin(prophecy_spark = prophecy_spark, v1 = "ddddsdad")]
             )], 
            pp, 
            Pp
        )
        pass
