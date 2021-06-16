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
        self.testfile = sys.argv[2]
        self.modelfile = sys.argv[3]
        self.outfile = sys.argv[4]
        self.cf_type = sys.argv[5]

    @staticmethod
    def get_usersimmap(rows):
        usm = {}

        for row in rows:
            user1, user2 = list(sorted((row["u1"], row["u2"])))
            sim = row["sim"]
            usm[(user1, user2)] = sim
        return usm

    @staticmethod
    def get_all_userids(usm):
        user_ids = set()
        for uid1, uid2 in usm:
            user_ids.add(uid1)
            user_ids.add(uid2)
        return user_ids

    @staticmethod
    def genubr(ubrlist):
        ubr = defaultdict(dict)
        for k, v in ubrlist.items():
            ubr[k][v[0]] = v[1]
        return ubr

    @staticmethod
    def get_item_prediction(uid, bid, ratings_list, modelMap):
        NEIGHBORS = 5
        valid_neighbors = []
        
        if uid not in ratings_list:
            return 0
        for bid2, rating in ratings_list[uid]:
            if tuple(sorted([bid, bid2])) in modelMap:
                valid_neighbors.append((rating, modelMap[tuple(sorted([bid, bid2]))]))
        
        valid_neighbors.sort(key=lambda x: x[1], reverse=True)
        valid_neighbors = valid_neighbors[:NEIGHBORS]

        top = bot = 0
        for x in valid_neighbors:
            top += x[0] * x[1]
            bot += abs(x[1])
        return 0 if (top == 0 or bot == 0) else top / bot

    @staticmethod
    def get_user_prediction(uid, bid, ratings_list, modelMap, ur_avg, ubMap):
        NEIGHBORS = 5
        valid_neighbors = []
        
        if bid not in ratings_list:
            return 0
        for uid2, rating in ratings_list[bid]:
            key = tuple(sorted([uid, uid2]))
            if key in modelMap:
                normal_rating = rating - ur_avg[uid2]
                valid_neighbors.append((normal_rating, modelMap[key]))
        
        valid_neighbors.sort(key=lambda x: x[1], reverse=True)
        valid_neighbors = valid_neighbors[:NEIGHBORS]

        top = bot = 0
        for x in valid_neighbors:
            top += x[0] * x[1]
            bot += abs(x[1])
        return 0 if (top == 0 or bot == 0) else (ur_avg[uid] + top / bot)

    def run_item_based(self):
        sc = SparkContext.getOrCreate()
        sc.setLogLevel("OFF")

        # {(b1, b2): sim}
        modelMap = sc.textFile(self.modelfile).map(lambda row: json.loads(row)).map(lambda row: (tuple(sorted([row["b1"], row["b2"]])), row["sim"])).collectAsMap()
        # {u1: {b1: 5, b2: 3}}
        textRDD = sc.textFile(self.ipf).map(lambda row: json.loads(row))
        ubr = textRDD.map(lambda row: (row["user_id"], (row["business_id"], row["stars"]))).groupByKey().map(lambda row: (row[0], set(row[1]))).collectAsMap()

        testRDD = sc.textFile(self.testfile).map(lambda row: json.loads(row))
        test_pairs = testRDD.map(lambda row: (row["user_id"], row["business_id"])).collect()
        
        with open(self.outfile, "w+") as f:
            for pair in test_pairs:
                uid = pair[0]
                bid = pair[1]
                rating = T3pred.get_item_prediction(uid, bid, ubr, modelMap)
                if rating:
                    f.write(json.dumps({"user_id": uid, "business_id": bid, "stars": rating}) + "\n")

    def run_user_based(self):
        sc = SparkContext.getOrCreate()
        sc.setLogLevel("OFF")

        # {(u1, u2): sim}
        modelMap = sc.textFile(self.modelfile).map(lambda row: json.loads(row)).map(lambda row: (tuple(sorted([row["u1"], row["u2"]])), row["sim"])).collectAsMap()
        
        # {u1: {b1: 5, b2: 3}}
        textRDD = sc.textFile(self.ipf).map(lambda row: json.loads(row))
        textRDD.cache()

        bur = textRDD.map(lambda row: (row["business_id"], (row["user_id"], row["stars"]))).groupByKey().map(lambda row: (row[0], set(row[1]))).collectAsMap()
        
        ur_avg = textRDD.map(lambda row: (row["user_id"], row["stars"])).groupByKey().mapValues(lambda row: list(row)).mapValues(lambda row: sum(row) / len(row)).collectAsMap()
        ubMap = textRDD.map(lambda row: ((row["user_id"], row["business_id"]), row["stars"])).collectAsMap()
        
        testRDD = sc.textFile(self.testfile).map(lambda row: json.loads(row))
        test_pairs = testRDD.map(lambda row: (row["user_id"], row["business_id"])).collect()
        
        with open(self.outfile, "w+") as f:
            for uid, bid in test_pairs:
                rating = T3pred.get_user_prediction(uid, bid, bur, modelMap, ur_avg, ubMap)
                if rating:
                    f.write(json.dumps({"user_id": uid, "business_id": bid, "stars": rating}) + "\n")

    def run(self):
        if self.cf_type == "user_based":
            self.run_user_based()
        else:
            self.run_item_based()


if __name__ == "__main__":
    t3 = T3pred()

    st = time.time()
    t3.run()
    et = time.time()
    print("Total time taken: {}".format(et - st))
