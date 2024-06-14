from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
import sys
import random
import threading
from collections import defaultdict

#Sticky Sampling global variables
epsilon = None
delta = None
num_elements = 0
bucket_size = None
data = defaultdict(int)
next_check = None

def sticky_sampling_init(epsilon_val, delta_val):
    global epsilon, delta, bucket_size, next_check
    epsilon = epsilon_val
    delta = delta_val
    bucket_size = int(1 / epsilon)
    next_check = bucket_size

def sticky_sampling_add(item):
    global num_elements, data, next_check, epsilon, bucket_size
    num_elements += 1
    if item in data:
        data[item] += 1
    else:
        if len(data) < bucket_size or random.random() < epsilon:
            data[item] = 1

    if num_elements >= next_check:
        sticky_sampling_remove_rare_items()
        next_check += bucket_size

def sticky_sampling_remove_rare_items():
    global data, epsilon, num_elements
    threshold = epsilon * num_elements
    keys_to_remove = [k for k, v in data.items() if v <= threshold]
    for k in keys_to_remove:
        del data[k]

def sticky_sampling_get_frequent_items():
    global data, epsilon, num_elements
    threshold = epsilon * num_elements
    return {k: v for k, v in data.items() if v > threshold}

#Operations to perform after receiving an RDD
def process_batch(time, batch):
    global streamLength, histogram, THRESHOLD
    batch_size = batch.count()
    #If we already have enough points, skip this batch.
    if streamLength[0] >= THRESHOLD:
        return

    streamLength[0] += batch_size
    #Extract the items from the batch
    batch_items = batch.map(lambda s: int(s)).collect()

    #Update the streaming state
    for item in batch_items:
        histogram[item] += 1
        sticky_sampling_add(item)

    if streamLength[0] >= THRESHOLD:
        stopping_condition.set()

if __name__ == '__main__':
    assert len(sys.argv) == 6, "USAGE: n, phi, epsilon, delta, port"

    n = int(sys.argv[1])
    phi = float(sys.argv[2])
    epsilon_val = float(sys.argv[3])
    delta = float(sys.argv[4])
    portExp = int(sys.argv[5])

    THRESHOLD = n

    conf = SparkConf().setMaster("local[*]").setAppName("FrequentItemsExample")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 1)
    ssc.sparkContext.setLogLevel("ERROR")

    stopping_condition = threading.Event()

    streamLength = [0] 
    #Hash Table for the distinct elements
    histogram = defaultdict(int) 
    sticky_sampling_init(epsilon_val, delta)

    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)
    stream.foreachRDD(lambda time, batch: process_batch(time, batch))

    ssc.start()
    stopping_condition.wait()
    ssc.stop(False, True)

    print(f"INPUT PROPERTIES\nn = {n} phi = {phi} epsilon = {epsilon_val} delta = {delta} port = {portExp}")

    print("STICKY SAMPLING")
    sticky_sampling_frequent_items = sticky_sampling_get_frequent_items()
    print("Number of items in the Hash Table =", len(data))
    print("Number of estimated frequent items =", len(sticky_sampling_frequent_items))
    print("Estimated frequent items:")

    for item in sorted(sticky_sampling_frequent_items.keys()):
        if histogram[item] >= phi * n:
            print(f"{item} +")
        else:
            print(f"{item} -")
