from collections import defaultdict, Counter
from itertools import combinations
from pyspark import SparkContext
import json
import re
import string
import sys
import time


class T2:
    def __init__(self) -> None:
        self.ipf = sys.argv[1]
        self.opf = sys.argv[2]
        self.stopwordsfile = sys.argv[3]

    @staticmethod
    def parse_review_list(row, stopwords):
        row = " ".join(row).lower()
        row = re.sub(string.punctuation + "[0-9]", row)
        row = re.compile("\w+").sub(lambda x: "" if x.group(0) in stopwords else x.group(0), row)
        ctr = Counter(row)
        total_wc = sum(ctr.values())
        threshold = 10 ** -6
        row = re.compile("\w+").sub(lambda x: "" if ctr[x.group(0)]/total_wc < threshold else x.group(0), row)


    def run(self):
        sc = SparkContext.getOrCreate()
        sc.setLogLevel("OFF")

        with open(self.stopwordsfile) as f:
            stopwords = set(map(lambda x: x.rstrip(), f.readlines()))
        
        textRDD = sc.textFile(self.ipf).map(lambda row: json.loads(row))
        textRDD.cache()

        biztextRDD = textRDD.map(lambda row: (row["business_id"], [row["text"]])).reduceByKey(lambda a, b: a + b).mapValues(lambda row: T2.parse_review_list(row, stopwords))

if __name__ == "__main__":
    t1 = T2()

    st = time.time()
    t1.run()
    et = time.time()
    print("Total time taken: {}".format(et - st))
