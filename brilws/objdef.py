class IOVPayloadItem(object): # to be replaced by cython!!
    def __init__(self,nfields=1):
        self.fields = [None]*nfields
    def nfields(self):
        return len(self.fields)
    def setfield(self,fieldidx,v):
        self.fields[fieldidx] = v
    def getfield(self,fieldidx=0):
        return self.fields[fieldidx]
    
class BCM1FChannelmaskItem(IOVPayloadItem): 
    def __init__(self):
        super(BCM1FChannelmaskItem, self).__init__()
        self.setfield(0,[1]*48)
    def maskchannels(self,channelids):
        for cid in channelids:
            self.fields[0][cid] = 0
            
class BHMLutItem(IOVPayloadItem):
    def __init__(self):
        super(BHMLutItem, self).__init__(2)
    def setlookup(self,key,val):
        self.fields[0] = key
        self.fields[1] = val

class HFLumiNormItem(IOVPayloadItem):
    def __init__(self):
        super(HFLumiNormItem, self).__init__()
    def setamodetag(self,val):
        self.fields[0] = val
    def setegev(self,val):
        self.fields[1] = val
    def setcomment(self,val):
        self.fileds[2] = val
    def setminbiasxsec(self,val):
        self.fields[3] = val
    def setfuncname(self,val):
        self.fields[4] = val
    def setfuncparams(self,val):
        self.fields[5] = val
    def setafterglow(self,cal):
        self.fields[6] = val
    def getamodetag(self):
        return self.fields[0]
    def getegev(self):
        return self.fields[1]
    def getcomment(self):
        return self.fields[2]
    def getminbiasxsef(self):
        return self.fields[3]
    def getfuncname(self):
        return self.fields[4]
    def getfuncparams(self):
        return self.fields[5]
    def getafterglow(self):
        return self.fields[6]
    
if __name__=='__main__':
    m = BCM1FChannelmaskItem()
    m.maskchannels([1,3,5])
    print m.getfield()
    bhmluts = [] 
    for i in xrange(40):
        l = BHMLutItem()
        l.setlookup(str(i),hex(i*100))
        bhmluts.append(l)
    print bhmluts

    
