from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import broadcast
import csv
import time

from SequentialFFT import SequentialFFT, d

def MRFFT(P, K):
    start_time = time.time()
    
    # R1
    def FFT(iterator):
        points = list(iterator)
        return SequentialFFT(points, K)

    coreset = P.mapPartitions(FFT).collect()

    r1_time = time.time() - start_time
    print("Round 1 time:", r1_time)
    
    # R2
    start_time = time.time()
    centers = SequentialFFT(coreset, K)
    r2_time = time.time() - start_time
    print("R2 time:", r2_time)
    
    
    broadcast_centers = sc.broadcast(centers)
    
    # R3
    start_time = time.time()
    max_distance = P.map(lambda x: max(d(x, c) for c in broadcast_centers.value)).reduce(max)
    r3_time = time.time() - start_time
    print("R3 time:", r3_time)

    return max_distance


if __name__ == "__main__":

    file_path = 'artificial1M_9_100.csv'
    K = 3

    conf = SparkConf().setAppName("MRFFT").setMaster("local[*]")
    sc = SparkContext.getOrCreate(conf)

    points = []
    with open(file_path, "r") as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            coord = tuple(map(float, row))
            points.append(coord)

    points_rdd = sc.parallelize(points)

    radius = MRFFT(points_rdd, K)
    print("Radius:", radius)

    sc.stop()
    