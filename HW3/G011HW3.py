from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
import sys
import random
import threading
from collections import defaultdict
import math 

#global variables
exact_items = defaultdict(int)
THRESHOLD = -1  # To be set via command line
reservoir = []
epsilon = None
delta = None
num_elements = 0
bucket_size = None
data = defaultdict(int)
next_check = None

#exact items
def exact_add(item):
    exact_items[item] += 1

def ExactAlgorithm():
    threshold = phi * len(exact_items)
    return {k: v for k, v in exact_items.items() if v > threshold}

#sticky sampling
def sticky_sampling_init(epsilon_val, delta_val):
    global epsilon, delta, bucket_size, next_check
    epsilon = epsilon_val
    delta = delta_val
    bucket_size = int(1 / epsilon)
    next_check = bucket_size

def sticky_sampling_add(item):
    global num_elements, data, next_check, epsilon, bucket_size, phi
    num_elements += 1
    if item in data:
        data[item] += 1
    else:
        sampling_rate = (math.log(1 / (delta * phi), math.e) / epsilon)/num_elements
        if len(data) < bucket_size or random.random() < sampling_rate:
            data[item] = 1

    if num_elements >= next_check:
        sticky_sampling_remove_rare_items()
        next_check += bucket_size

def sticky_sampling_remove_rare_items():
    global data, epsilon, phi, num_elements
    threshold = (phi - epsilon) * num_elements
    keys_to_remove = [k for k, v in data.items() if v < threshold]
    for k in keys_to_remove:
        del data[k]
    for k, v in data.items():
        print(k, ":", v, "t", threshold)

def sticky_sampling_get_frequent_items():
    global data, epsilon, num_elements
    threshold = epsilon * num_elements
    return {k: v for k, v in data.items() if v > threshold}

#Operations to perform after receiving an RDD
def process_batch(time, batch):
    global streamLength, reservoir, m
    batch_size = batch.count()
    
    # If we already have enough points (> THRESHOLD), skip this batch.
    if streamLength[0] >= THRESHOLD:
        return
    streamLength[0] += batch_size
    
    # Extract items from the batch
    batch_items = batch.map(lambda s: int(s)).collect()

    #Update the streaming state
    for item in batch_items:
        exact_add(item)
        sticky_sampling_add(item)
    
    # update the reservoir
    for item in batch_items:
        if len(reservoir) < m:
            reservoir.append(item)
        else:
            j = random.randint(0, streamLength[0] - 1)
            if j < m:
                reservoir[j] = item

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
    m = math.ceil(1 / phi)

    conf = SparkConf().setMaster("local[*]").setAppName("FrequentItemsExample")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 1)
    ssc.sparkContext.setLogLevel("ERROR")

    stopping_condition = threading.Event()

    streamLength = [0] 
    sticky_sampling_init(epsilon_val, delta)

    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)
    stream.foreachRDD(lambda time, batch: process_batch(time, batch))

    ssc.start()
    stopping_condition.wait()
    ssc.stop(False, True)

    print(f"INPUT PROPERTIES\nn = {n} phi = {phi} epsilon = {epsilon_val} delta = {delta} port = {portExp}")

    print("EXACT ALGORITHM")
    true_frequent_items = ExactAlgorithm()
    print("Number of items in the data structure =", len(exact_items))
    print("Number of true frequent items =", len(true_frequent_items))
    print("True frequent items:")

    for item in sorted(true_frequent_items):
        print(f"{item}")
        
    print("RESERVOIR SAMPLING")
    print(f"Size m of the sample = {m}")
    
    # determine frequent 
    estimated_frequent_items = set(reservoir)
    print(f"Number of estimated frequent items = {len(estimated_frequent_items)}")
        
    print("Estimated frequent items:")
    for item in sorted(estimated_frequent_items):
        if item in true_frequent_items:
            print(f"{item} +")
        else:
            print(f"{item} -")

    print("STICKY SAMPLING")
    sticky_sampling_frequent_items = sticky_sampling_get_frequent_items()
    print("Number of items in the Hash Table =", len(data))
    print("Number of estimated frequent items =", len(sticky_sampling_frequent_items))
    print("Estimated frequent items:")

    for item in sorted(sticky_sampling_frequent_items.keys()):
        if item in true_frequent_items:
            print(f"{item} +")
        else:
            print(f"{item} -")