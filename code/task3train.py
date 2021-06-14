from collections import defaultdict, Counter
from itertools import combinations
from pyspark import SparkContext
import json
import random
import sys
import time


class T3t:
    def __init__(self) -> None:
        self.trainfile = sys.argv[1]
        self.outmodelfile = sys.argv[2]
        self.cf_type = sys.argv[3]

    def run(self):
        sc = SparkContext.getOrCreate()
        sc.setLogLevel("OFF")

    @staticmethod
    def gen_hash_fns(num_funcs):
        step = 10 ** 7
        curstep = step
        start = 10 ** 9 - 10 ** 8 + 17
        fns = [None] * num_funcs

        for i in range(num_funcs):
            a = random.randint(start, start + curstep - 10 ** 7 + 23)
            b = random.randint(10 ** 9 - 10 ** 8 + 30697,
                               10 ** 9 + curstep + 10 ** 8 - 4123892)
            curstep += step
            fns[i] = (a, b)
        return fns

    @staticmethod
    def gen_signatures(uid_values, hash_params, num_buckets):
        p = 10 ** 9 + 7
        sig = []
        for a, b in hash_params:
            sig_row = []
            for v in uid_values:
                sig_row.append(((a * v + b) % p) % num_buckets)
            sig.append(min(sig_row))
        return sig

if __name__ == "__main__":
    t3 = T3t()

    st = time.time()
    t3.run()
    et = time.time()
    print("Total time taken: {}".format(et - st))
