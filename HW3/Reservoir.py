from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
import threading
import sys
import random
import math

# After how many items should we stop?
THRESHOLD = -1  # To be set via command line

reservoir = []

# # I imagine these are the true frequen items we got from ExactAlgorithm
# def ExactAlgorithm():
#     return {195773912, 339323283, 434415286, 641486445, 819911327, 870070186, 1472610405, 1590293530, 1690049656, 1936875793}


# Operations to perform after receiving an RDD 'batch' at time 'time'
def process_batch(time, batch):
    global streamLength, reservoir, m
    batch_size = batch.count()
    
    # If we already have enough points (> THRESHOLD), skip this batch.
    if streamLength[0] >= THRESHOLD:
        return
    streamLength[0] += batch_size
    
    # Extract items from the batch
    batch_items = batch.map(lambda s: int(s)).collect()
    
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
    assert len(sys.argv) == 6, "USAGE: GxxxHW3.py <n> <phi> <epsilon> <delta> <port>"
    

    n = int(sys.argv[1])
    phi = float(sys.argv[2])
    epsilon = float(sys.argv[3])
    delta = float(sys.argv[4])
    portExp = int(sys.argv[5])
    
    THRESHOLD = n
    m = math.ceil(1 / phi) 
    
    conf = SparkConf().setMaster("local[*]").setAppName("ReservoirSampling")
    
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 0.01)  # Batch duration of 0.01 seconds
    ssc.sparkContext.setLogLevel("ERROR")
    
    stopping_condition = threading.Event()
    
    print("INPUT PROPERTIES")
    print(f"n = {n} phi = {phi} epsilon = {epsilon} delta = {delta} port = {portExp}")
    
    streamLength = [0]  # Stream length (an array to be passed by reference)
    
    # CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)
    stream.foreachRDD(lambda time, batch: process_batch(time, batch))
    
    # MANAGING STREAMING SPARK CONTEXT
    ssc.start()
    stopping_condition.wait()
    ssc.stop(False, True)
    
    print("RESERVOIR SAMPLING")
    print(f"Size m of the sample = {m}")
    
    # determine frequent 
    estimated_frequent_items = set(reservoir)
    print(f"Number of estimated frequent items = {len(estimated_frequent_items)}")
    
    # true frequent items (Exact)
    true_frequent_items = ExactAlgorithm()
    

    print("Estimated frequent items:")
    for item in sorted(estimated_frequent_items):
        if item in true_frequent_items:
            print(f"{item} +")
        else:
            print(f"{item} -")
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
import threading
import sys
import random
import math

# After how many items should we stop?
THRESHOLD = -1  # To be set via command line

reservoir = []

# # I imagine these are the true frequen items we got from ExactAlgorithm
# def ExactAlgorithm():
#     return {195773912, 339323283, 434415286, 641486445, 819911327, 870070186, 1472610405, 1590293530, 1690049656, 1936875793}


# Operations to perform after receiving an RDD 'batch' at time 'time'
def process_batch(time, batch):
    global streamLength, reservoir, m
    batch_size = batch.count()
    
    # If we already have enough points (> THRESHOLD), skip this batch.
    if streamLength[0] >= THRESHOLD:
        return
    streamLength[0] += batch_size
    
    # Extract items from the batch
    batch_items = batch.map(lambda s: int(s)).collect()
    
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
    assert len(sys.argv) == 6, "USAGE: GxxxHW3.py <n> <phi> <epsilon> <delta> <port>"
    

    n = int(sys.argv[1])
    phi = float(sys.argv[2])
    epsilon = float(sys.argv[3])
    delta = float(sys.argv[4])
    portExp = int(sys.argv[5])
    
    THRESHOLD = n
    m = math.ceil(1 / phi) 
    
    conf = SparkConf().setMaster("local[*]").setAppName("ReservoirSampling")
    
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 0.01)  # Batch duration of 0.01 seconds
    ssc.sparkContext.setLogLevel("ERROR")
    
    stopping_condition = threading.Event()
    
    print("INPUT PROPERTIES")
    print(f"n = {n} phi = {phi} epsilon = {epsilon} delta = {delta} port = {portExp}")
    
    streamLength = [0]  # Stream length (an array to be passed by reference)
    
    # CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)
    stream.foreachRDD(lambda time, batch: process_batch(time, batch))
    
    # MANAGING STREAMING SPARK CONTEXT
    ssc.start()
    stopping_condition.wait()
    ssc.stop(False, True)
    
    print("RESERVOIR SAMPLING")
    print(f"Size m of the sample = {m}")
    
    # determine frequent 
    estimated_frequent_items = set(reservoir)
    print(f"Number of estimated frequent items = {len(estimated_frequent_items)}")
    
    # true frequent items (Exact)
    true_frequent_items = ExactAlgorithm()
    

    print("Estimated frequent items:")
    for item in sorted(estimated_frequent_items):
        if item in true_frequent_items:
            print(f"{item} +")
        else:
            print(f"{item} -")
