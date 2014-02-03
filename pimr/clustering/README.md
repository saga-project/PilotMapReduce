# DEBUGGING and Profiling

    python kmeans_map_partition.py chunkFileName 8 centers.txt

    the first argument to map partiton script is chunk file
    2nd argument - number of reduces
    centers file


    time python kmeans_map_partition.py  /work/01131/tg804093/input-all/random_10000000points.csv 1 applications/centers.txt 
    17:04:20 - Total number of datapoints/chunk is 10000000 
    17:06:30 - Total Time taken - 129.87
    
    real    2m59.720s
    user    2m52.745s
    sys 0m2.836s
    
    (python)tg804093@login1.stampede /work/01131/tg804093/src/PilotMapReduce/pimr/clustering$ time python kmeans_map_partition_numpy.py  /work/01131/tg804093/input-all/random_10000000points.csv 1 applications/centers.txt 
    lapack_info:
      NOT AVAILABLE
    lapack_opt_info:
      NOT AVAILABLE
    blas_info:
      NOT AVAILABLE
    atlas_threads_info:
      NOT AVAILABLE
    blas_src_info:
      NOT AVAILABLE
    atlas_blas_info:
      NOT AVAILABLE
    lapack_src_info:
      NOT AVAILABLE
    openblas_info:
      NOT AVAILABLE
    atlas_blas_threads_info:
      NOT AVAILABLE
    blas_mkl_info:
      NOT AVAILABLE
    blas_opt_info:
      NOT AVAILABLE
    atlas_info:
      NOT AVAILABLE
    lapack_mkl_info:
      NOT AVAILABLE
    mkl_info:
      NOT AVAILABLE
    
    
    real    11m10.531s
    user    10m59.756s
    sys 0m2.562s
    
    
    export PYTHONPATH=../../:~/.bigjob/python/lib/python2.7/site-packages/saga_python-0-py2.7.egg/:~/.bigjob/python/lib/python2.7/site-packages/radical.utils-0.5-py2.7.egg/:~/.bigjob/python/lib/python2.7/site-packages/pudb-2013.5.1-py2.7.egg/:~/.bigjob/python/lib/python2.7/site-packages/BigJob2-0.54_42_g09ea8bc-py2.7.egg/:~/.bigjob/python/lib/python2.7/site-packages/redis-2.9.1-py2.7.egg:~/.bigjob/python/lib/python2.7/site-packages/tldextract-1.3.1-py2.7.egg/:~/.bigjob/python/lib/python2.7/site-packages/pexpect-3.1-py2.7.egg
    
    tg804093@login1.stampede /work/01131/tg804093/src/PilotMapReduce/pimr/clustering$ time python kmeans_map_partition_numpy.py  /work/01131/tg804093/input-all/random_10000000points.csv 1 applications/centers.txt 
    lapack_opt_info:
        libraries = ['mkl_lapack95_lp64', 'mkl_intel_lp64', 'mkl_intel_thread', 'mkl_core', 'iomp5', 'pthread']
        library_dirs = ['/home/builder/master/lib']
        define_macros = [('SCIPY_MKL_H', None)]
        include_dirs = ['/home/builder/master/include']
    blas_opt_info:
        libraries = ['mkl_intel_lp64', 'mkl_intel_thread', 'mkl_core', 'iomp5', 'pthread']
        library_dirs = ['/home/builder/master/lib']
        define_macros = [('SCIPY_MKL_H', None)]
        include_dirs = ['/home/builder/master/include']
    lapack_mkl_info:
        libraries = ['mkl_lapack95_lp64', 'mkl_intel_lp64', 'mkl_intel_thread', 'mkl_core', 'iomp5', 'pthread']
        library_dirs = ['/home/builder/master/lib']
        define_macros = [('SCIPY_MKL_H', None)]
        include_dirs = ['/home/builder/master/include']
    blas_mkl_info:
        libraries = ['mkl_intel_lp64', 'mkl_intel_thread', 'mkl_core', 'iomp5', 'pthread']
        library_dirs = ['/home/builder/master/lib']
        define_macros = [('SCIPY_MKL_H', None)]
        include_dirs = ['/home/builder/master/include']
    mkl_info:
        libraries = ['mkl_intel_lp64', 'mkl_intel_thread', 'mkl_core', 'iomp5', 'pthread']
        library_dirs = ['/home/builder/master/lib']
        define_macros = [('SCIPY_MKL_H', None)]
        include_dirs = ['/home/builder/master/include']
    17:55:26 - Total number of datapoints/chunk is 10000000 
    
    real    8m56.760s
    user    8m47.255s
    sys 0m2.176s
