import tarfile
import os
source_dir = 'run1data_1634_198271'
output_filename = 'run1data_1634_198271.tar.gz'
def make_tarfile(output_filename, source_dir):
    '''
    build a .tar.gz for an entire directory tree
    '''
    with tarfile.open(output_filename, 'w:gz') as tar:        
        tar.add(source_dir, arcname=os.path.basename(source_dir))

make_tarfile(output_filename,source_dir)
