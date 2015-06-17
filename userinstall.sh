if [ -z "$1" ]
then
    prefix=$HOME
else
    prefix=$1    
fi
echo "installing brilws in $prefix"
mkdir -p ${prefix}/lib/python2.7/site-packages
export PYTHONPATH=${prefix}/lib/python2.7/site-packages:$PYTHONPATH  
python setup.py install --prefix=${prefix}
export PATH=${prefix}/bin:$PATH