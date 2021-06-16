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
        NEIGHBORS = 100
        valid_neighbors = []
        
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
    def get_user_predictions(pair, ratings_list, usm):
        biz_id, user_id = pair
        neighbor_uids = set(map(lambda rating: rating[0], ratings_list))

        best_N_neighbors = []
        NEIGHBORS = 5
        for cand in neighbor_uids:
            user_pair = tuple(sorted(user_id, cand))
            if user_pair in usm:
                best_N_neighbors.append((user_id, cand, usm[user_pair]))
        best_N_neighbors.sort(reverse=True, key=lambda x: x[2])
        best_N_neighbors = best_N_neighbors[:NEIGHBORS]
        avg_biz_rating = sum([ur[1] for ur in ratings_list]) / len(ratings_list)

    def run_item_based(self):
        sc = SparkContext.getOrCreate()
        sc.setLogLevel("OFF")

        # {(b1, b2): sim}
        modelMap = sc.textFile(self.modelfile).map(lambda row: json.loads(row)).map(lambda row: (tuple(sorted([row["b1"], row["b2"]])), row["sim"])).collectAsMap()
        # {u1: {b1: 5, b2: 3}}
        ubr = sc.textFile(self.ipf).map(lambda row: json.loads(row)).map(lambda row: (row["user_id"], (row["business_id"], row["stars"]))).groupByKey().map(lambda row: (row[0], set(row[1]))).collectAsMap()

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
        return
        sc = SparkContext.getOrCreate()
        sc.setLogLevel("OFF")
        
        # biz: user-rating {biz: ([user, stars], [...], ...)}
        bizuserstar = sc.textFile(self.ipf).map(lambda review: json.loads(review)).map(lambda review: (review["business_id"], [(review["user_id"], review["stars"])])).reduceByKey(lambda ur1, ur2: ur1 + ur2).collectAsMap()

        # user-pair: sim {(u1, u2): sim}
        usm = sc.textFile(self.modelfile).map(lambda mod: json.loads(mod)).map(lambda mod: ((mod["u1"], mod["u2"]), mod["sim"])).collectAsMap()

        # {u1, u2, ...}
        unique_uids = T3pred.get_all_userids(usm)

        testRDD = sc.textFile(self.testfile).map(lambda review: json.loads(review)).map(lambda review: (review["business_id"], review["user_id"])).filter(lambda pair: pair[1] in bizuserstar and pair[0] in unique_uids)

        predicted_ratings = testRDD.map(lambda pair: T3pred.get_user_predictions(pair, bizuserstar[pair[0]], usm))
        

        # User-based CF begins here

        # [(u1, b1), ...]

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
