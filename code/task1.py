from itertools import combinations
from pyspark import SparkContext
import json
import random
import sys
import time


class T1:
    def __init__(self) -> None:
        self.ipf = sys.argv[1]
        self.opf = sys.argv[2]
        self.sc = SparkContext.getOrCreate()
        self.sc.setLogLevel("OFF")

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
        sig = [None] * len(uid_values)
        
        for a, b in hash_params:
            sig_row = [None] * len(hash_params)
            for v in uid_values:
                sig_row.append(((a * v + b) % p) % num_buckets)
            sig.append(min(sig_row))
        return sig

    def write_data(self, actual_pairs):
        with open(self.opf, "w+") as f:
            for b1, b2, sim in actual_pairs:
                f.write(json.dumps({'b1': b1, 'b2': b2, 'sim': sim}) + "\n")

    def run(self):
        num_bands = 100
        num_hashes = 100
        textRDD = self.sc.textFile(self.ipf).map(lambda row: json.loads(row))
        textRDD.cache()

        userRDD = textRDD.map(lambda row: row["user_id"]).distinct()
        cmp_map = userRDD.zipWithIndex().collectAsMap()
        num_buckets = len(cmp_map) - 1
        hash_params = T1.gen_hash_fns(num_hashes)

        # (biz_id, [uid1, uid2, uid3, uid17, uid50, ...])
        biz_sets = textRDD.map(lambda row: (row['business_id'], {cmp_map[row['user_id']]})).distinct(
        ).reduceByKey(lambda x, y: x.update(y)).map(lambda row: (row[0], list(row[1])))
        biz_sets.cache()
        biz_map = biz_sets.mapValues(lambda uids: set(uids)).collectAsMap()

        sig_m = biz_sets.mapValues(lambda uids_list: T1.gen_signatures(uids_list, hash_params, num_buckets)).collect()
        
        candidate_pairs = set()

        for _ in range(num_bands):
            current_bucket = set()
            for sig in sig_m:
                current_bucket.add(sig[0])
            for bizz in current_bucket:
                if len(bizz) > 1:
                    for b1, b2 in combinations(sorted(bizz), 2):
                        candidate_pairs.add((b1, b2))
        
        actual_similar_bizz = []
        for b1, b2 in candidate_pairs:
            u1 = biz_map[b1]
            u2 = biz_map[b2]

            sim = len(u1 & u2) / len(u1 | u2)
            if sim >= 0.05:
                actual_similar_bizz.append((b1, b2, sim))
        self.write_data(actual_similar_bizz)

if __name__ == "__main__":
    t1 = T1()

    st = time.time()
    t1.run()
    et = time.time()
    print("Total time taken: {}".format(et - st))
