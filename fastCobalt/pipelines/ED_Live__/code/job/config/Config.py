from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            jdbcUrl_P1_COBALTHS_NYUEDICD: str=None,
            username_P1_COBALTHS_NYUEDICD: str=None,
            password_P1_COBALTHS_NYUEDICD: str=None,
            jdbcUrl_P1_COBALTHS_NYUEDICD_384: str=None,
            username_P1_COBALTHS_NYUEDICD_384: str=None,
            password_P1_COBALTHS_NYUEDICD_384: str=None,
            jdbcUrl_P1_COBALTHS_CCI_Scor: str=None,
            username_P1_COBALTHS_CCI_Scor: str=None,
            password_P1_COBALTHS_CCI_Scor: str=None,
            jdbcUrl_P1_COBALTHS_ED_Drivi: str=None,
            username_P1_COBALTHS_ED_Drivi: str=None,
            password_P1_COBALTHS_ED_Drivi: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(
            jdbcUrl_P1_COBALTHS_NYUEDICD, 
            username_P1_COBALTHS_NYUEDICD, 
            password_P1_COBALTHS_NYUEDICD, 
            jdbcUrl_P1_COBALTHS_NYUEDICD_384, 
            username_P1_COBALTHS_NYUEDICD_384, 
            password_P1_COBALTHS_NYUEDICD_384, 
            jdbcUrl_P1_COBALTHS_CCI_Scor, 
            username_P1_COBALTHS_CCI_Scor, 
            password_P1_COBALTHS_CCI_Scor, 
            jdbcUrl_P1_COBALTHS_ED_Drivi, 
            username_P1_COBALTHS_ED_Drivi, 
            password_P1_COBALTHS_ED_Drivi
        )

    def update(
            self,
            jdbcUrl_P1_COBALTHS_NYUEDICD: str="Z:\\Alteryx\\Jon\\Prophecy Test\\ED\\ENC_ED Files20240228015756\\P1.COBALTHS.NYU ED ICD_10.txt",
            username_P1_COBALTHS_NYUEDICD: str="",
            password_P1_COBALTHS_NYUEDICD: str="",
            jdbcUrl_P1_COBALTHS_NYUEDICD_384: str="Z:\\Alteryx\\Jon\\Prophecy Test\\ED\\ENC_ED Files20240228015756\\P1.COBALTHS.NYU ED ICD_10.txt",
            username_P1_COBALTHS_NYUEDICD_384: str="",
            password_P1_COBALTHS_NYUEDICD_384: str="",
            jdbcUrl_P1_COBALTHS_CCI_Scor: str="Z:\\Alteryx\\Jon\\Prophecy Test\\ED\\ENC_ED Files20240228015756\\P1.COBALTHS.CCI_Scores.yxdb",
            username_P1_COBALTHS_CCI_Scor: str="",
            password_P1_COBALTHS_CCI_Scor: str="",
            jdbcUrl_P1_COBALTHS_ED_Drivi: str="Z:\\Alteryx\\Jon\\Prophecy Test\\ED\\ENC_ED Files20240228015756\\P1.COBALTHS.ED_Driving_Distance.yxdb",
            username_P1_COBALTHS_ED_Drivi: str="",
            password_P1_COBALTHS_ED_Drivi: str="",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.jdbcUrl_P1_COBALTHS_NYUEDICD = jdbcUrl_P1_COBALTHS_NYUEDICD
        self.username_P1_COBALTHS_NYUEDICD = username_P1_COBALTHS_NYUEDICD
        self.password_P1_COBALTHS_NYUEDICD = password_P1_COBALTHS_NYUEDICD
        self.jdbcUrl_P1_COBALTHS_NYUEDICD_384 = jdbcUrl_P1_COBALTHS_NYUEDICD_384
        self.username_P1_COBALTHS_NYUEDICD_384 = username_P1_COBALTHS_NYUEDICD_384
        self.password_P1_COBALTHS_NYUEDICD_384 = password_P1_COBALTHS_NYUEDICD_384
        self.jdbcUrl_P1_COBALTHS_CCI_Scor = jdbcUrl_P1_COBALTHS_CCI_Scor
        self.username_P1_COBALTHS_CCI_Scor = username_P1_COBALTHS_CCI_Scor
        self.password_P1_COBALTHS_CCI_Scor = password_P1_COBALTHS_CCI_Scor
        self.jdbcUrl_P1_COBALTHS_ED_Drivi = jdbcUrl_P1_COBALTHS_ED_Drivi
        self.username_P1_COBALTHS_ED_Drivi = username_P1_COBALTHS_ED_Drivi
        self.password_P1_COBALTHS_ED_Drivi = password_P1_COBALTHS_ED_Drivi
        pass
