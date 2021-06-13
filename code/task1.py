from collections import defaultdict
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
        sig = []
        for a, b in hash_params:
            sig_row = []
            for v in uid_values:
                sig_row.append(((a * v + b) % p) % num_buckets)
            sig.append(min(sig_row))
        return sig

    def write_data(self, actual_pairs):
        with open(self.opf, "w+") as f:
            for b1, b2, sim in actual_pairs:
                f.write(json.dumps({'b1': b1, 'b2': b2, 'sim': sim}) + "\n")

    @staticmethod
    def split_bands(whole_array, num_bands):
        ret = []
        cur_band_num = 0
        for i in range(0, len(whole_array), num_bands):
            ret.append((cur_band_num, hash(tuple(whole_array[i: i + num_bands]))))
            cur_band_num += 1
        return ret

    @staticmethod
    def sb2(A, nb):
        ret = []
        cur_band_num = 0
        for i in range(0, len(A), nb):
            ret.append(hash(tuple(A[i: i + nb])))
            cur_band_num += 1
        return ret

    def run(self):
        num_bands = 100
        num_hashes = 100
        textRDD = self.sc.textFile(self.ipf).map(lambda row: json.loads(row))
        textRDD.cache()

        userRDD = textRDD.map(lambda row: row["user_id"]).distinct()
        cmp_map = userRDD.zipWithIndex().collectAsMap()
        num_buckets = len(cmp_map) - 1
        hash_params = T1.gen_hash_fns(num_hashes)

        # (biz_id, {uid1, uid2, uid3, uid17, uid50, ...})
        biz_sets = textRDD.map(lambda row: (row['business_id'], cmp_map[row['user_id']])).distinct(
        ).groupByKey().map(lambda row: (row[0], set(row[1])))
        biz_sets.cache()
        biz_map = biz_sets.collectAsMap()

        sig_temp = biz_sets.mapValues(lambda uids_list: T1.gen_signatures(uids_list, hash_params, num_buckets)).collect()
        # print(sig_temp[:10])
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
            # print("Processed band: {}".format(i))
        

        # for band_num in range(num_bands):
        #     current_bucket = defaultdict(set)
        #     for sig in chunks[band_num]:
        #         # print("Sig for band: {}, {}".format(sig, band_num))
        #         hash_val = hash(tuple(sig[1]))
        #         current_bucket[hash_val].add(sig[0])
        #     # print("Processing band: {}".format(band_num))
        #     # print("Number of hash buckets: {}".format(len(current_bucket)))
        #     for v in current_bucket.values():
        #         # print("Len of current hashvalues: {}".format(len(v)))
        #         if len(v) == 1:
        #             continue
        #         for b1, b2 in combinations(sorted(current_bucket), 2):
        #             print("Potential candidates: ", b1, b2)
        #             candidate_pairs.add((b1, b2))

        actual_similar_bizz = []
        print("Number of candidate pairs: {}".format(len(candidate_pairs)))
        for b1, b2 in candidate_pairs:
            u1 = biz_map[b1]
            u2 = biz_map[b2]

            sim = len(u1 & u2) / len(u1 | u2)
            if sim >= 0.05:
                actual_similar_bizz.append((b1, b2, sim))
        self.write_data(actual_similar_bizz)
        return
        
        
        
        cands1 = sig_temp.flatMap(lambda item: [(tuple(x), item[0]) for x in T1.split_bands(item[1], num_bands)]).groupByKey().map(lambda item: list(item[1]))
        
        print("~~~ Cands1 ~~~")
        i = 0
        for k in cands1.collect():
            if len(k) > 1:
                print(k)
            

        cands2 = cands1.filter(lambda count: len(count) > 1)
        cands3 = cands2.collect()
        candidate_pairs = cands3
        
        
        # flatMap(lambda bizz: [bizz2 for bizz2 in combinations(bizz, 2)]).collect()
        # print("Reached sig_m generated")
        # print("Number of rows in sig_m: {}".format(len(sig_m)))
        # candidate_pairs = set()
        # chunks = T1.split_bands(sig_m, num_bands)
        # print("First chunk items: {}".format(chunks[0][:10]))

        


if __name__ == "__main__":
    t1 = T1()

    st = time.time()
    t1.run()
    et = time.time()
    print("Total time taken: {}".format(et - st))
