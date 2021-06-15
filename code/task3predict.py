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

    @staticmethod
    def get_usersimmap(rows):
        usm = {}

        for row in rows:
            user1, user2 = list(sorted(row["u1"], row["u2"]))
            sim = row["sim"]
            usm[(user1, user2)] = sim
        return usm

    @staticmethod
    def get_all_userids(usm):
        user_ids = set()
        for uid1, uid2 in usm:
            set.add(uid1)
            set.add(uid2)
        return user_ids

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
        


    def run(self):
        sc = SparkContext.getOrCreate()
        sc.setLogLevel("OFF")
        
        bizuserstar = sc.textFile(self.ipf).map(lambda review: json.loads(review)).map(lambda review: (review["business_id"], [(review["user_id"], review["stars"])])).reduceByKey(lambda ur1, ur2: ur1 + ur2).collectAsMap()
        user_sims = sc.textFile(self.modelfile).map(lambda mod: json.loads(mod)).collect()
        usm = T3pred.get_usersimmap(user_sims)
        unique_uids = T3pred.get_all_userids(usm)

        testRDD = sc.textFile(self.testfile).map(lambda review: json.loads(review)).map(lambda review: (review["business_id"], review["user_id"])).filter(lambda pair: pair[1] in bizuserstar and pair[0] in unique_uids)

        predicted_ratings = testRDD.map(lambda pair: T3pred.get_user_predictions(pair, bizuserstar[pair[0]], usm))
        

        # User-based CF begins here
        # user-pair: sim {(u1, u2): sim}
        # biz: user-rating {biz: (user, stars)}
        # [(u1, b1), ...]


if __name__ == "__main__":
    t3 = T3pred()

    st = time.time()
    t3.run()
    et = time.time()
    print("Total time taken: {}".format(et - st))
