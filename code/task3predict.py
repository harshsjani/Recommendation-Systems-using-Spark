from collections import defaultdict
from itertools import combinations
from pyspark import SparkContext
from math import sqrt
import json
import random
import sys
import time


class T3pred:
    def __init__(self) -> None:
        self.ipf = sys.argv[1]
        self.testfile = sys.argv[1]
        self.modelfile = sys.argv[3]
        self.outfile = sys.argv[4]
        self.cf_type = sys.argv[5]

    def run(self):
        sc = SparkContext.getOrCreate()
        sc.setLogLevel("OFF")
        
        trainRDD = sc.textFile(self.ipf).map(lambda review: json.loads(review)).map(lambda review: (review["user_id"], review["business_id"], review["stars"]))
        testRDD = sc.textFile(self.testfile).map(lambda review: json.loads(review)).map(lambda review: (review["user_id"], review["business_id"]))
        modelRDD = sc.textFile(self.modelfile).map(lambda mod: json.loads(mod))

        # User-based CF begins here
        

if __name__ == "__main__":
    t3 = T3pred()

    st = time.time()
    t3.run()
    et = time.time()
    print("Total time taken: {}".format(et - st))
