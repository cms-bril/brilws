import tarfile
import os
import re
import subprocess

def make_tarfile(output_filename, source_dir):
    '''
    build a .tar.gz for an entire directory tree
    '''
    with tarfile.open(output_filename, 'w:gz') as tar:        
        tar.add(source_dir, arcname=os.path.basename(source_dir))
        
def upload_eos(sourcefile,eos_dest,eos_prefix='root://eoscms.cern.ch/'):
    '''
    xrdcp sourcefile $eos_prefix/$eosdest/$sourcefile
    '''
    cmmd = ['xrdcp',sourcefile,eos_prefix+'/'+eos_dest+'/'+sourcefile]
    #output = subprocess.check_output(cmmd)
    exitCode = subprocess.call(cmmd)
    return exitCode

eos_dest='/eos/cms/store/user/xiezhen/lumidb'
dirpattern =re.compile('^run1data_*')
dirs = [f for f in os.listdir('.') if os.path.isdir(f) and dirpattern.match(f)]
for d in dirs:    
    source_dir = d
    output_filename = d+'.tar.gz'
    print 'making tar file %s'%output_filename
    make_tarfile(output_filename,source_dir)
    print 'moving %s to eos %s'%(output_filename,eos_dest)
    ecode = upload_eos(output_filename,eos_dest)
    if int(ecode)==0:
        print 'deleting %s'%output_filename
        os.remove(output_filename)
