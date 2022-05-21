from datetime import datetime
import io

class Logger:
    def __init__(self):
        pass

    def log(self, file_obj, message):
        try:
            with open(str(file_obj), 'a') as file:
                file.write(str(datetime.now())+ ":  "+message+"\n")

        except io.UnsupportedOperation as e:
            print(e)
        except Exception as e:
            raise e