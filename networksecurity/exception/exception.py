import sys
from types import TracebackType
from networksecurity.logging import logger

class NetworkSecurityException(Exception):
    def __init__(self,error_message,error_detail):
        self.error_message = error_message
        _,_,exc_tb = error_detail.exc_info()
        self.lineno = exc_tb.tb_lineno if exc_tb is not None else None
        self.filename = exc_tb.tb_frame.f_code.co_filename if exc_tb is not None else None

    def __str__(self):
        return "Error occurred in script: {0} at line number: {1} with message: {2}".format(
            self.filename,
            self.lineno,
            self.error_message
        )
if __name__ == "__main__":
    try:
        logger.logging.info("Enter the try block")
        a = 1 / 0
        print("This will not be printed",a)
    except Exception as e:
        ns_exception = NetworkSecurityException(e,sys)
        print(ns_exception)