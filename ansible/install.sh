curl -o anaconda.sh https://repo.continuum.io/archive/Anaconda2-4.2.0-Linux-x86_64.sh;
bash anaconda.sh -p anaconda -b;
rm anaconda.sh;
anaconda/bin/pip install eventlet;
