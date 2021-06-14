from pyspark import SparkContext
from math import sqrt
import json
import sys
import time


class T2p:
    def __init__(self) -> None:
        self.testfile = sys.argv[1]
        self.modelfile = sys.argv[2]
        self.outputfile = sys.argv[3]

    @staticmethod
    def cosine_sim(uservec, bizvec):
        if not uservec or bizvec:
            return 0
        return len(uservec & bizvec) / (sqrt(len(uservec)) * sqrt(len(bizvec)))

    def run(self):
        sc = SparkContext.getOrCreate()
        sc.setLogLevel("OFF")
        
        reviewRDD = sc.textFile(self.modelfile).map(lambda row: json.loads(row)).map(lambda row: (row["user_id", row["business_id"]]))
        modelRDD = sc.textFile(self.modelfile).map(lambda row: json.loads(row))

        uzp = {x[1] : set(x[2]) for x in modelRDD.filter(lambda row: row["type"] == "user").collect()}
        bzp = {x[1] : set(x[2]) for x in modelRDD.filter(lambda row: row["type"] == "biz").collect()}
        
        ans = sc.textFile(self.modelfile).map(lambda row: json.loads(row)).map(lambda row: (row["user_id", row["business_id"]])).map(lambda x, y: (x, y, T2p.cosine_sim(uzp.get(x), bzp.get(y)))).filter(lambda x: x[2] >= 0.01).collect()
        
        with open(self.outputfile, "w+") as f:
            for row in ans:
                f.write(json.dumps({"user_id": row[0], "business_id": row[1], "sim": row[2]}) + "\n")


if __name__ == "__main__":
    t2p = T2p()

    st = time.time()
    t2p.run()
    et = time.time()
    print("Total time taken: {}".format(et - st))
