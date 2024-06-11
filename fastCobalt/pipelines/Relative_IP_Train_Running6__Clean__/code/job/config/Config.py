from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            jdbcUrl_DSN_edw_prod_Query_S: str=None,
            username_DSN_edw_prod_Query_S: str=None,
            password_DSN_edw_prod_Query_S: str=None,
            jdbcUrl_DSN_edw_prod_Query_S_2346: str=None,
            username_DSN_edw_prod_Query_S_2346: str=None,
            password_DSN_edw_prod_Query_S_2346: str=None,
            jdbcUrl_DSN_edw_prod_Query_S_2350: str=None,
            username_DSN_edw_prod_Query_S_2350: str=None,
            password_DSN_edw_prod_Query_S_2350: str=None,
            jdbcUrl_CCI_Scores_yxdb: str=None,
            username_CCI_Scores_yxdb: str=None,
            password_CCI_Scores_yxdb: str=None,
            jdbcUrl_DSN_edw_prod_Query_S_2678: str=None,
            username_DSN_edw_prod_Query_S_2678: str=None,
            password_DSN_edw_prod_Query_S_2678: str=None,
            jdbcUrl_DSN_edw_prod_Query_S_2688: str=None,
            username_DSN_edw_prod_Query_S_2688: str=None,
            password_DSN_edw_prod_Query_S_2688: str=None,
            jdbcUrl_DSN_edw_prod_Query_S_2634: str=None,
            username_DSN_edw_prod_Query_S_2634: str=None,
            password_DSN_edw_prod_Query_S_2634: str=None,
            jdbcUrl_DSN_edw_prod_Query_S_761: str=None,
            username_DSN_edw_prod_Query_S_761: str=None,
            password_DSN_edw_prod_Query_S_761: str=None,
            jdbcUrl_DSN_edw_prod_Query_S_737: str=None,
            username_DSN_edw_prod_Query_S_737: str=None,
            password_DSN_edw_prod_Query_S_737: str=None,
            jdbcUrl_DSN_edw_prod_Query_S_2781: str=None,
            username_DSN_edw_prod_Query_S_2781: str=None,
            password_DSN_edw_prod_Query_S_2781: str=None,
            jdbcUrl_DSN_edw_prod_Query_S_2308: str=None,
            username_DSN_edw_prod_Query_S_2308: str=None,
            password_DSN_edw_prod_Query_S_2308: str=None,
            jdbcUrl_DSN_edw_prod_Query_s: str=None,
            username_DSN_edw_prod_Query_s: str=None,
            password_DSN_edw_prod_Query_s: str=None,
            jdbcUrl_DSN_edw_prod_Query_S_2766: str=None,
            username_DSN_edw_prod_Query_S_2766: str=None,
            password_DSN_edw_prod_Query_S_2766: str=None,
            jdbcUrl_DSN_edw_prod_Query_s_543: str=None,
            username_DSN_edw_prod_Query_s_543: str=None,
            password_DSN_edw_prod_Query_s_543: str=None,
            jdbcUrl_DSN_edw_prod_Query_S_620: str=None,
            username_DSN_edw_prod_Query_S_620: str=None,
            password_DSN_edw_prod_Query_S_620: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(
            jdbcUrl_DSN_edw_prod_Query_S, 
            username_DSN_edw_prod_Query_S, 
            password_DSN_edw_prod_Query_S, 
            jdbcUrl_DSN_edw_prod_Query_S_2346, 
            username_DSN_edw_prod_Query_S_2346, 
            password_DSN_edw_prod_Query_S_2346, 
            jdbcUrl_DSN_edw_prod_Query_S_2350, 
            username_DSN_edw_prod_Query_S_2350, 
            password_DSN_edw_prod_Query_S_2350, 
            jdbcUrl_CCI_Scores_yxdb, 
            username_CCI_Scores_yxdb, 
            password_CCI_Scores_yxdb, 
            jdbcUrl_DSN_edw_prod_Query_S_2678, 
            username_DSN_edw_prod_Query_S_2678, 
            password_DSN_edw_prod_Query_S_2678, 
            jdbcUrl_DSN_edw_prod_Query_S_2688, 
            username_DSN_edw_prod_Query_S_2688, 
            password_DSN_edw_prod_Query_S_2688, 
            jdbcUrl_DSN_edw_prod_Query_S_2634, 
            username_DSN_edw_prod_Query_S_2634, 
            password_DSN_edw_prod_Query_S_2634, 
            jdbcUrl_DSN_edw_prod_Query_S_761, 
            username_DSN_edw_prod_Query_S_761, 
            password_DSN_edw_prod_Query_S_761, 
            jdbcUrl_DSN_edw_prod_Query_S_737, 
            username_DSN_edw_prod_Query_S_737, 
            password_DSN_edw_prod_Query_S_737, 
            jdbcUrl_DSN_edw_prod_Query_S_2781, 
            username_DSN_edw_prod_Query_S_2781, 
            password_DSN_edw_prod_Query_S_2781, 
            jdbcUrl_DSN_edw_prod_Query_S_2308, 
            username_DSN_edw_prod_Query_S_2308, 
            password_DSN_edw_prod_Query_S_2308, 
            jdbcUrl_DSN_edw_prod_Query_s, 
            username_DSN_edw_prod_Query_s, 
            password_DSN_edw_prod_Query_s, 
            jdbcUrl_DSN_edw_prod_Query_S_2766, 
            username_DSN_edw_prod_Query_S_2766, 
            password_DSN_edw_prod_Query_S_2766, 
            jdbcUrl_DSN_edw_prod_Query_s_543, 
            username_DSN_edw_prod_Query_s_543, 
            password_DSN_edw_prod_Query_s_543, 
            jdbcUrl_DSN_edw_prod_Query_S_620, 
            username_DSN_edw_prod_Query_S_620, 
            password_DSN_edw_prod_Query_S_620
        )

    def update(
            self,
            jdbcUrl_DSN_edw_prod_Query_S: str="odbc:DSN=edw_prod;UID=u301737;PWD=__EncPwd1__",
            username_DSN_edw_prod_Query_S: str="",
            password_DSN_edw_prod_Query_S: str="",
            jdbcUrl_DSN_edw_prod_Query_S_2346: str="odbc:DSN=edw_prod;UID=u301737;PWD=__EncPwd1__",
            username_DSN_edw_prod_Query_S_2346: str="",
            password_DSN_edw_prod_Query_S_2346: str="",
            jdbcUrl_DSN_edw_prod_Query_S_2350: str="odbc:DSN=edw_prod;UID=u301737;PWD=__EncPwd1__",
            username_DSN_edw_prod_Query_S_2350: str="",
            password_DSN_edw_prod_Query_S_2350: str="",
            jdbcUrl_CCI_Scores_yxdb: str="O:\\library\\Alteryx\\Advanced Analytics\\Reference Workflows\\CCI\\CCI_Scores.yxdb",
            username_CCI_Scores_yxdb: str="",
            password_CCI_Scores_yxdb: str="",
            jdbcUrl_DSN_edw_prod_Query_S_2678: str="odbc:DSN=edw_prod;UID=u301737;PWD=__EncPwd1__",
            username_DSN_edw_prod_Query_S_2678: str="",
            password_DSN_edw_prod_Query_S_2678: str="",
            jdbcUrl_DSN_edw_prod_Query_S_2688: str="odbc:DSN=edw_prod;UID=u301737;PWD=__EncPwd1__",
            username_DSN_edw_prod_Query_S_2688: str="",
            password_DSN_edw_prod_Query_S_2688: str="",
            jdbcUrl_DSN_edw_prod_Query_S_2634: str="odbc:DSN=edw_prod;UID=u301737;PWD=__EncPwd1__",
            username_DSN_edw_prod_Query_S_2634: str="",
            password_DSN_edw_prod_Query_S_2634: str="",
            jdbcUrl_DSN_edw_prod_Query_S_761: str="odbc:DSN=edw_prod;UID=u301737;PWD=__EncPwd1__",
            username_DSN_edw_prod_Query_S_761: str="",
            password_DSN_edw_prod_Query_S_761: str="",
            jdbcUrl_DSN_edw_prod_Query_S_737: str="odbc:DSN=edw_prod;UID=u301737;PWD=__EncPwd1__",
            username_DSN_edw_prod_Query_S_737: str="",
            password_DSN_edw_prod_Query_S_737: str="",
            jdbcUrl_DSN_edw_prod_Query_S_2781: str="odbc:DSN=edw_prod;UID=u301737;PWD=__EncPwd1__",
            username_DSN_edw_prod_Query_S_2781: str="",
            password_DSN_edw_prod_Query_S_2781: str="",
            jdbcUrl_DSN_edw_prod_Query_S_2308: str="odbc:DSN=edw_prod;UID=u301737;PWD=__EncPwd1__",
            username_DSN_edw_prod_Query_S_2308: str="",
            password_DSN_edw_prod_Query_S_2308: str="",
            jdbcUrl_DSN_edw_prod_Query_s: str="odbc:DSN=edw_prod;UID=u301737;PWD=__EncPwd1__",
            username_DSN_edw_prod_Query_s: str="",
            password_DSN_edw_prod_Query_s: str="",
            jdbcUrl_DSN_edw_prod_Query_S_2766: str="odbc:DSN=edw_prod;UID=u301737;PWD=__EncPwd1__",
            username_DSN_edw_prod_Query_S_2766: str="",
            password_DSN_edw_prod_Query_S_2766: str="",
            jdbcUrl_DSN_edw_prod_Query_s_543: str="odbc:DSN=edw_prod;UID=u301737;PWD=__EncPwd1__",
            username_DSN_edw_prod_Query_s_543: str="",
            password_DSN_edw_prod_Query_s_543: str="",
            jdbcUrl_DSN_edw_prod_Query_S_620: str="odbc:DSN=edw_prod;UID=u301737;PWD=__EncPwd1__",
            username_DSN_edw_prod_Query_S_620: str="",
            password_DSN_edw_prod_Query_S_620: str="",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.jdbcUrl_DSN_edw_prod_Query_S = jdbcUrl_DSN_edw_prod_Query_S
        self.username_DSN_edw_prod_Query_S = username_DSN_edw_prod_Query_S
        self.password_DSN_edw_prod_Query_S = password_DSN_edw_prod_Query_S
        self.jdbcUrl_DSN_edw_prod_Query_S_2346 = jdbcUrl_DSN_edw_prod_Query_S_2346
        self.username_DSN_edw_prod_Query_S_2346 = username_DSN_edw_prod_Query_S_2346
        self.password_DSN_edw_prod_Query_S_2346 = password_DSN_edw_prod_Query_S_2346
        self.jdbcUrl_DSN_edw_prod_Query_S_2350 = jdbcUrl_DSN_edw_prod_Query_S_2350
        self.username_DSN_edw_prod_Query_S_2350 = username_DSN_edw_prod_Query_S_2350
        self.password_DSN_edw_prod_Query_S_2350 = password_DSN_edw_prod_Query_S_2350
        self.jdbcUrl_CCI_Scores_yxdb = jdbcUrl_CCI_Scores_yxdb
        self.username_CCI_Scores_yxdb = username_CCI_Scores_yxdb
        self.password_CCI_Scores_yxdb = password_CCI_Scores_yxdb
        self.jdbcUrl_DSN_edw_prod_Query_S_2678 = jdbcUrl_DSN_edw_prod_Query_S_2678
        self.username_DSN_edw_prod_Query_S_2678 = username_DSN_edw_prod_Query_S_2678
        self.password_DSN_edw_prod_Query_S_2678 = password_DSN_edw_prod_Query_S_2678
        self.jdbcUrl_DSN_edw_prod_Query_S_2688 = jdbcUrl_DSN_edw_prod_Query_S_2688
        self.username_DSN_edw_prod_Query_S_2688 = username_DSN_edw_prod_Query_S_2688
        self.password_DSN_edw_prod_Query_S_2688 = password_DSN_edw_prod_Query_S_2688
        self.jdbcUrl_DSN_edw_prod_Query_S_2634 = jdbcUrl_DSN_edw_prod_Query_S_2634
        self.username_DSN_edw_prod_Query_S_2634 = username_DSN_edw_prod_Query_S_2634
        self.password_DSN_edw_prod_Query_S_2634 = password_DSN_edw_prod_Query_S_2634
        self.jdbcUrl_DSN_edw_prod_Query_S_761 = jdbcUrl_DSN_edw_prod_Query_S_761
        self.username_DSN_edw_prod_Query_S_761 = username_DSN_edw_prod_Query_S_761
        self.password_DSN_edw_prod_Query_S_761 = password_DSN_edw_prod_Query_S_761
        self.jdbcUrl_DSN_edw_prod_Query_S_737 = jdbcUrl_DSN_edw_prod_Query_S_737
        self.username_DSN_edw_prod_Query_S_737 = username_DSN_edw_prod_Query_S_737
        self.password_DSN_edw_prod_Query_S_737 = password_DSN_edw_prod_Query_S_737
        self.jdbcUrl_DSN_edw_prod_Query_S_2781 = jdbcUrl_DSN_edw_prod_Query_S_2781
        self.username_DSN_edw_prod_Query_S_2781 = username_DSN_edw_prod_Query_S_2781
        self.password_DSN_edw_prod_Query_S_2781 = password_DSN_edw_prod_Query_S_2781
        self.jdbcUrl_DSN_edw_prod_Query_S_2308 = jdbcUrl_DSN_edw_prod_Query_S_2308
        self.username_DSN_edw_prod_Query_S_2308 = username_DSN_edw_prod_Query_S_2308
        self.password_DSN_edw_prod_Query_S_2308 = password_DSN_edw_prod_Query_S_2308
        self.jdbcUrl_DSN_edw_prod_Query_s = jdbcUrl_DSN_edw_prod_Query_s
        self.username_DSN_edw_prod_Query_s = username_DSN_edw_prod_Query_s
        self.password_DSN_edw_prod_Query_s = password_DSN_edw_prod_Query_s
        self.jdbcUrl_DSN_edw_prod_Query_S_2766 = jdbcUrl_DSN_edw_prod_Query_S_2766
        self.username_DSN_edw_prod_Query_S_2766 = username_DSN_edw_prod_Query_S_2766
        self.password_DSN_edw_prod_Query_S_2766 = password_DSN_edw_prod_Query_S_2766
        self.jdbcUrl_DSN_edw_prod_Query_s_543 = jdbcUrl_DSN_edw_prod_Query_s_543
        self.username_DSN_edw_prod_Query_s_543 = username_DSN_edw_prod_Query_s_543
        self.password_DSN_edw_prod_Query_s_543 = password_DSN_edw_prod_Query_s_543
        self.jdbcUrl_DSN_edw_prod_Query_S_620 = jdbcUrl_DSN_edw_prod_Query_S_620
        self.username_DSN_edw_prod_Query_S_620 = username_DSN_edw_prod_Query_S_620
        self.password_DSN_edw_prod_Query_S_620 = password_DSN_edw_prod_Query_S_620
        pass
