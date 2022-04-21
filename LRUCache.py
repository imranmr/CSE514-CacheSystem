import collections
import random
import time
import tqdm


class LRUCache:
    def __init__(self, size):
        self.size = size
        self.lru_cache = collections.OrderedDict()
        self.hit = 0
        self.totalrequest= 0
        self.num_evicted=0

    def get(self, key):
        self.totalrequest+=1
        try:# hit
            value = self.lru_cache.pop(key) #removes it
            self.lru_cache[key] = value     #reinsert it
            self.hit+=1
            return value
        except KeyError:# miss
            return -1

    def put(self, key, value):
        try: #removes if key already inserted
            self.lru_cache.pop(key)
            self.num_evicted += 1
        except KeyError:
            if len(self.lru_cache) >= self.size: #removes based on LRU
                self.lru_cache.popitem(last=False)
                self.num_evicted += 1
        self.lru_cache[key] = value

    def show_entries(self):
        print(self.lru_cache)

    def cache_size(self):
        print("Cache Size:",len(self.lru_cache))

    def hitratio(self):
        print("Hit Ratio:",1.00 * self.hit/self.totalrequest)

    def numeviction(self):
        print("Total Evictions:",self.num_evicted)

    def missratio(self):
        print("Miss Ratio:",1-1.00 * self.hit/self.totalrequest)




# 2 random number generators
# 1 for inserting
# 1 for getting
# heapsize = 1048576000
start_time = time.time()
heapsize = 100000000
numofitems=0
maincache = LRUCache(heapsize)

for i in tqdm.tqdm(range(heapsize)):#Inserting data into cache
    randnum = random.randrange(start=1,stop=heapsize*2,step=1)
    maincache.put(randnum, randnum)

for i in tqdm.tqdm(range(heapsize)):#test hit ratio
    randnum = random.randrange(start=1, stop=heapsize*2, step=1)
    maincache.get(randnum)

for i in tqdm.tqdm(range(heapsize)):#test evictions
    randnum = random.randrange(start=1,stop=heapsize*2,step=1)
    maincache.put(randnum, randnum)

maincache.numeviction()
maincache.cache_size() #Cache size might be smaller due to random numbers repeating
maincache.hitratio()
maincache.missratio()
print("--- %s seconds ---" % (time.time() - start_time))
