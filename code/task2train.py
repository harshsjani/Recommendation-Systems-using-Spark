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
        row = "".join([x for x in row if x == " " or x.isalpha()])
        row = re.compile("\w+").sub(lambda x: "" if x.group(0) in stopwords else x.group(0), row)
        ctr = Counter(row.split())
        total_wc = sum(ctr.values())
        highest_wc = max(ctr.values())
        threshold = 10 ** -6
        row = re.compile("\w+").sub(lambda x: "" if ctr[x.group(0)]/total_wc < threshold else x.group(0), row)
        tf = {}
        for word, count in ctr:
            tf[word] = count / highest_wc
        return row, tf


    def run(self):
        sc = SparkContext.getOrCreate()
        sc.setLogLevel("OFF")

        with open(self.stopwordsfile) as f:
            stopwords = set(map(lambda x: x.rstrip(), f.readlines()))
        
        textRDD = sc.textFile(self.ipf).map(lambda row: json.loads(row))
        textRDD.cache()

        biztextRDD = textRDD.map(lambda row: (row["business_id"], [row["text"]])).reduceByKey(lambda a, b: a + b).mapValues(lambda row: T2.parse_review_list(row, stopwords))
        biztextRDD.cache()
        # (  business_id, ([word1, word2, ...], tf_dict)  )
        biz_count = biztextRDD.count()
        idf_map = biztextRDD.flatMap(lambda row: [(word, 1) for word, _ in Counter(row[1][0])]).reduceByKey(lambda x, y: x + y).map()
        

if __name__ == "__main__":
    t1 = T2()

    st = time.time()
    t1.run()
    et = time.time()
    print("Total time taken: {}".format(et - st))
