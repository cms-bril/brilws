from brilws import api
import sys
r=api.parsecmsselectJSON('3')
print r[0]
if len(sys.argv)>1:
    args = sys.argv[1]
    r=api.parsecmsselectJSON(args)
    print r
