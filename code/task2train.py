from collections import defaultdict, Counter
from itertools import combinations
from pyspark import SparkContext
import json
from math import log2
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
        highest_wc = max(ctr.values())
        tf = defaultdict(int)
        for word, count in ctr.items():
            tf[word] = count / highest_wc
        return row.split(), tf

    @staticmethod
    def get_biz_profile(row, idf_map):
        wlist = row[0]
        tf = row[1]

        tfidfs = []

        for word in set(wlist):
            tfidf = tf[word] * idf_map[word]
            tfidfs.append((word, tfidf))
        tfidfs.sort(reverse=True, key=lambda x: x[1])
        return list(map(lambda x: x[0], tfidfs[:200]))

    @staticmethod
    def bizlist_to_wordvec(biz_list, biz_profile):
        words = set()
        for biz in biz_list:
            words.update(set(biz_profile[biz]))
        return list(words)[:500]

    def run(self):
        sc = SparkContext.getOrCreate()
        sc.setLogLevel("OFF")

        with open(self.stopwordsfile) as f:
            stopwords = set(map(lambda x: x.rstrip(), f.readlines()))
        
        textRDD = sc.textFile(self.ipf).map(lambda row: json.loads(row))
        textRDD.cache()

        # (  business_id, ([word1, word2, ...], tf_dict)  )
        biztextRDD = textRDD.map(lambda row: (row["business_id"], [row["text"]])).reduceByKey(lambda a, b: a + b).mapValues(lambda row: T2.parse_review_list(row, stopwords))
        biztextRDD.cache()
        
        biz_count = biztextRDD.count()
        # ({word: count, ...})
        idf_map = biztextRDD.flatMap(lambda row: [(word, 1) for word, _ in Counter(row[1][0]).items()]).reduceByKey(lambda x, y: x + y).map(lambda kv: (kv[0], log2(biz_count / kv[1]))).collectAsMap()
        
        # { biz_id: word_list, ... }
        biz_profile = biztextRDD.mapValues(lambda x: T2.get_biz_profile(x, idf_map)).collectAsMap()
        biztextRDD.unpersist()
        del idf_map
        user_profile = textRDD.map(lambda row: (row["user_id"], [row["business_id"]])).reduceByKey(lambda x, y: x + y).mapValues(lambda value: T2.bizlist_to_wordvec(value, biz_profile)).collectAsMap()
        textRDD.unpersist()
        
        with open(self.opf, "w+") as f:
            for k, v in biz_profile.items():
                f.write(json.dumps({"type": "biz", "biz_id": k, "fvec": v}) + "\n")
            for k, v in user_profile.items():
                f.write(json.dumps({"type": "user", "user_id": k, "fvec": v}) + "\n")


if __name__ == "__main__":
    t1 = T2()

    st = time.time()
    t1.run()
    et = time.time()
    print("Total time taken: {}".format(et - st))
