import numpy as np

# Size of output file will be NUMBER_CLUSTERS * NUMBER_POINTS_PER_CLUSTER * 32 Byte
NUMBER_CLUSTERS=8
NUMBER_POINTS_PER_CLUSTER=6710886.4

for n in range(NUMBER_CLUSTERS):
    number_points = NUMBER_POINTS_PER_CLUSTER*NUMBER_CLUSTERS*(2**n)
    OUTPUT_FILE="random_data_%.1fGB.csv"%((number_points*32.0)/(1024*1024*1024))
    print "Number points: %d Generate: %s"%(number_points, OUTPUT_FILE)

    np.random.seed(seed=1234567)     
    centers = 10 * np.random.rand(NUMBER_CLUSTERS, 2)
    print "Centers: " + str(centers)
    random_data = []
    
    f = open(OUTPUT_FILE, "w")
    
    for i in range(0, NUMBER_CLUSTERS):
        data = 10*np.random.randn(NUMBER_POINTS_PER_CLUSTER*(2**n), 2) + centers[i]
        
        print "Cluster %d: %s"%(i,str(data))
        for d in data:
            f.write(str(d[0]) +"," + str(d[1]) + "\n")
        
        
    f.close()
