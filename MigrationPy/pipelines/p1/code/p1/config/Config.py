from p1.graph.Subgraph_1.config.Config import SubgraphConfig as Subgraph_1_Config
from p1.graph.Subgraph_2.config.Config import SubgraphConfig as Subgraph_2_Config
from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, Subgraph_1: dict=None, Subgraph_2: dict=None, **kwargs):
        self.spark = None
        self.update(Subgraph_1, Subgraph_2)

    def update(self, Subgraph_1: dict={}, Subgraph_2: dict={}, **kwargs):
        prophecy_spark = self.spark
        self.Subgraph_1 = self.get_config_object(
            prophecy_spark, 
            Subgraph_1_Config(prophecy_spark = prophecy_spark), 
            Subgraph_1, 
            Subgraph_1_Config
        )
        self.Subgraph_2 = self.get_config_object(
            prophecy_spark, 
            Subgraph_2_Config(prophecy_spark = prophecy_spark), 
            Subgraph_2, 
            Subgraph_2_Config
        )
        pass
