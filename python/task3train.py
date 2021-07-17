from collections import defaultdict
from itertools import combinations
from pyspark import SparkContext
from math import sqrt
import json
import random
import sys
import time


class T3t:
    def __init__(self) -> None:
        self.trainfile = sys.argv[1]
        self.outmodelfile = sys.argv[2]
        self.cf_type = sys.argv[3]

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

    @staticmethod
    def gen_candidate_pairs(sig_temp, num_bands):
        candidate_pairs = set()
        for i in range(num_bands):
            cur_bucket = defaultdict(set)
            for row in sig_temp:
                cur_bucket[row[1][i]].add(row[0])
            for v in cur_bucket.values():
                if len(v) == 1:
                    continue
                for b1, b2 in combinations(sorted(v), 2):                    
                    candidate_pairs.add((b1, b2))
        return candidate_pairs

    @staticmethod
    def get_ps(rating1, rating2):
        k1set = set(rating1.keys())
        k2set = set(rating2.keys())
        intsc = k1set.intersection(k2set)

        c1 = []
        c2 = []
        for c in intsc:
            c1.append(rating1[c])
            c2.append(rating2[c])
        mean1 = (sum(c1) / len(intsc))
        mean2 = (sum(c2) / len(intsc))
        rbaru = [-mean1 + r for r in c1]
        rbarv = [-mean2 + r for r in c2]
        num = sum([rbaru[i] * rbarv[i] for i in range(len(rbaru))])
        bot = sqrt(sum([pow(x, 2) for x in rbaru])) * sqrt(sum([pow(x, 2) for x in rbarv]))
        return 0 if not (num > 0 and bot > 0) else num / bot

    @staticmethod
    def get_actual_pairs(candidates, ubr):
        actual_similar_bizz = []
        print("Number of candidate pairs: {}".format(len(candidates)))
        for u1, u2 in candidates:
            rating1 = ubr[u1]
            rating2 = ubr[u2]
            k1set = set(rating1.keys())
            k2set = set(rating2.keys())

            intsc = k1set & k2set
            if len(intsc) >= 3:
                sim = len(intsc) / len(k1set | k2set)
                if sim >= 0.01:
                    ps = T3t.get_ps(rating1, rating2)
                    if ps > 0:
                        actual_similar_bizz.append((u1, u2, ps))
        return actual_similar_bizz

    @staticmethod
    def gen_burmap(burmap):
        ret = defaultdict(dict)

        for biz_id, value in burmap.items():
            for u, r in value:
                ret[biz_id][u] = r
        return ret

    def run_user_based(self):
        st = time.time()
        sc = SparkContext.getOrCreate()
        sc.setLogLevel("OFF")

        num_bands = 70
        num_hashes = 70
        textRDD = sc.textFile(self.trainfile).map(lambda row: json.loads(row))
        textRDD.cache()

        bizRDD = textRDD.map(lambda row: row["business_id"]).distinct()
        cmp_map = bizRDD.zipWithIndex().collectAsMap()
        num_buckets = len(cmp_map) - 1
        hash_params = T3t.gen_hash_fns(num_hashes)

        # (user_id, {biz1, biz2, ...})
        biz_sets = textRDD.map(lambda row: (row['user_id'], cmp_map[row['business_id']])).distinct(
        ).groupByKey().map(lambda row: (row[0], set(row[1])))
        biz_sets.cache()

        sig_temp = biz_sets.mapValues(lambda uids_list: T3t.gen_signatures(uids_list, hash_params, num_buckets)).collect()
        
        candidate_pairs = T3t.gen_candidate_pairs(sig_temp, num_bands)

        ubRDD = textRDD.map(lambda row: (row["user_id"], [(row["business_id"], row["stars"])])).reduceByKey(lambda x, y: x + y).collect()
        ubr = {row[0] : {x[0] : x[1] for x in row[1]} for row in ubRDD}
        print("Running time before actual pairs: {}".format(time.time() - st))
        actual_pairs = T3t.get_actual_pairs(candidate_pairs, ubr)
        
        with open(self.outmodelfile, "w+") as f:
            for useru, userv, sim in actual_pairs:
                f.write(json.dumps({"u1": useru, "u2": userv, "sim": sim}) + "\n")

    def run_item_based(self):
        sc = SparkContext.getOrCreate()
        sc.setLogLevel("OFF")

        ubrRDD = sc.textFile(self.trainfile).map(lambda row: json.loads(row)).map(lambda row: (row["business_id"], (row["user_id"], row["stars"])))
        burMap = T3t.gen_burmap(ubrRDD.groupByKey().map(lambda row: (row[0], list(row[1]))).filter(lambda row: len(row[1]) >= 3).collectAsMap())
        unique_biz = burMap.keys()

        similar_bizz = []
        for biz1, biz2 in combinations(unique_biz, 2):
            rating1 = burMap[biz1]
            rating2 = burMap[biz2]

            intsc = set(rating1.keys()).intersection(set(rating2.keys()))
            if len(intsc) > 2:
                ps = T3t.get_ps(rating1, rating2)
                if ps > 0:
                    similar_bizz.append((biz1, biz2, ps))
        
        with open(self.outmodelfile, "w+") as f:
            for biz1, biz2, ps in similar_bizz:
                f.write(json.dumps({"b1": biz1, "b2": biz2, "sim": ps}) + "\n")

    def run(self):
        if self.cf_type == "user_based":
            self.run_user_based()
        else:
            self.run_item_based()

if __name__ == "__main__":
    t3 = T3t()

    st = time.time()
    t3.run()
    et = time.time()
    print("Total time taken: {}".format(et - st))
