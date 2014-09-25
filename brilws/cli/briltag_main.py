import sys,logging,os
import docopt
import schema
import brilws

log = logging.getLogger('briltag')
logformatter = logging.Formatter('%(levelname)s %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.WARNING)
#fh = logging.FileHandler('/tmp/briltag.log')
ch.setFormatter(logformatter)
#fh.setFormatter(logformatter)
log.addHandler(ch)
#log.addHandler(fh)

choice_sources = ['BHM','BCM1F','PLT','HFOC','PIXEL']
choice_applyto = ['LUMI','BKG']

def briltag_main():
    docstr='''

    usage:
      briltag (-h|--help) 
      briltag --version
      briltag --checkforupdate
      briltag [--debug|--nowarning] <command> [<args>...]

    commands:
      norm Norm tag 
      data Data tag
      lut  Lut tag
      
    See 'briltag <command> --help' for more information on a specific command.

    '''
    
    args = {}
    argv = sys.argv[1:]
    args = docopt.docopt(docstr,argv,help=True,version=brilws.__version__,options_first=True)
    
    if '--debug' in sys.argv:
       log.setLevel(logging.DEBUG)
       ch.setLevel(logging.DEBUG)
    if args['--version'] : print brilws.__version__
    log.debug('global arguments: %s',args)
    cmmdargv = [args['<command>']] + args['<args>']

    log.debug('command arguments: %s',cmmdargv)
    parseresult = {}

    try:
      if args['<command>'] == 'norm':
         import briltag_norm
         parseresult = docopt.docopt(briltag_norm.__doc__,argv=cmmdargv)
         parseresult = briltag_norm.validate(parseresult,sources=choice_sources,applyto=choice_applyto)
         
      elif args['<command>'] == 'lut':
         import briltag_lut
         parseresult = docopt.docopt(briltag_lut.__doc__,argv=cmmdargv)
         parseresult = briltag_lut.validate(parseresult,sources=choice_sources)

      elif args['<command>'] == 'data':
         import briltag_data
         parseresult = docopt.docopt(briltag_data.__doc__,argv=cmmdargv)
         parseresult = briltag_data.validate(parseresult)

      else:
          exit("%r is not a briltag command. See 'briltag --help'."%args['<command>']) 
    except docopt.DocoptExit:
      raise docopt.DocoptExit('Error: incorrect input format for '+args['<command>'])            
    except schema.SchemaError as e:
      exit(e)

    if not parseresult.has_key('--debug'):
       if parseresult.has_key('--nowarning'):
          log.setLevel(logging.ERROR)
          ch.setLevel(logging.ERROR)
    else:
       log.setLevel(logging.DEBUG)
       ch.setLevel(logging.DEBUG)
       log.debug('create arguments: %s',parseresult)
    log.debug('create arguments: %s',parseresult)

if __name__ == '__main__':
    briltag_main()
