class ZfsErrException(Exception):
    
    def __init__(self,errtype='default',errcode='000'):
        Exception.__init__()
        self.err_type=errtype
        self.err_code=errcode
    
    def ErrMessage(self,msg=''):
        if msg=='':
            return