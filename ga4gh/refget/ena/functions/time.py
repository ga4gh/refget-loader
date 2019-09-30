import datetime

def timestamp():
    fmt = "%Y-%m-%dT%H:%M:%S"
    return datetime.datetime.now().strftime(fmt)
