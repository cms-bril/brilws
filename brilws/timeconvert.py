from datetime import datetime, timedelta
import time

nbx = 3564
orbits_per_ls = 262144
bunchspace_us = 0.02495
bunchspace_s = 24.95e-09

def OrbitDuration(norbits = 1):
    return timedelta(microseconds=(nbx*norbits*bunchspace_us))

def LSDuration(norbitsperls = orbits_per_ls):
    return OrbitDuration(norbitsperls)

def OrbitToTime(begtimetuple,orbitnumber,begorbit=0):
    return begtimetuple+OrbitDuration(orbitnumber-begorbit)

def DatetimeToSec(dt, epoch=datetime(1970,1,1)):
    td = dt - epoch
    return int(td.total_seconds())

def SecToDatetime(tssecs,utc=True):
    if utc:
        return datetime.utcfromtimestamp(int(tssecs))
    else:
        return datetime.fromtimestamp(int(tssecs))
    
def SecToDatetimeStr(tssecs,utc=True,fmt='%m/%d/%y %H:%M:%S'):
    return SecToDatetime(tssecs,utc=utc).strftime(fmt)

if __name__ == '__main__':
    tsec = DatetimeToSec(datetime.utcnow())
    print tsec
    t = SecToDatetime(tsec)
    print t
    tp = t.timetuple()
    print tp.tm_year,tp.tm_mon,tp.tm_wday,tp.tm_yday
    print SecToDatetimeStr(tsec)
    print LSDuration()
    print LSDuration().seconds, LSDuration().microseconds
