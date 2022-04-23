import time
import sys
import random
import os

class ListNode(object):
    def __init__(self, key, value, ttl=-1, freq=0):
        self.key = key #object name
        self.value = value #object size
        self.freq = freq 
        self.ttl = ttl #-1 ttl is infinity


class LFUCache(object):

    def __init__(self, capacity, overhead=0):
        """
        :type capacity: int
        :type timer: int
        :type overhead: int
        """
        self.__capa = capacity #maximum size of cache in bytes
        self.__size = 0
        self.__min_freq = 0
        #self.__freq_to_nodes = collections.defaultdict(LinkedList)
        self.__key_to_node = {}
        self.__overhead = overhead #overhead for each item in bytes
        self.__timer = 0 #keeps track of time for TTL expiration detection
        
        #variables below are for statistics
        self.__evictions = 0 #keeps track of items evicted that are not from ttl expiration
        self.__hits = 0
        self.__misses = 0
        self.__ttl_expirations = 0
        self.__insertions = 0


    def get(self, key):
        """
        :type key: int
        :rtype: int
        """
        if key not in self.__key_to_node: #item not in cache
            self.__misses += 1
            return -1
        elif (self.__key_to_node[key].ttl < self.__timer): #item has expired
            #print(f"timer {self.__timer}    ttl {self.__key_to_node[key].ttl}")
            self.__size -= self.__key_to_node[key].value + self.__overhead #update cache size
            self.__ttl_expirations += 1
            self.__misses += 1
            self.__key_to_node.pop(key) #delete expired node
            return -2
        

        self.__key_to_node[key].freq += 1 #update frequency
        self.__hits += 1
        return self.__key_to_node[key].value
        

    def put(self, key, value, current_time, ttl = 0): #value is size of the object
        """
        :type key: int
        :type value: int
        :rtype: void
        """
        if current_time < self.__timer:
            print("ERROR TIMER")
        if current_time > self.__timer: #update timer
            self.__timer = current_time

        if (ttl == 0): #if ttl is 0 no need to cache
            return
            
        if key in self.__key_to_node: #if key already exits then update ttl
            if (self.__key_to_node[key].value != value):
                print(f"ERROR {self.__key_to_node[key].value} != {value}")
                exit()
            self.__key_to_node[key].ttl = ttl + self.__timer
            return

        if (self.__capa < self.__size + value + self.__overhead): #if item will not fit
            #try removing expired ttl
            #self.__key_to_node.sort(key=lambda node: node.ttl)
            #remove expired ttl items
            for k, v in list(self.__key_to_node.items()): #TODO make more efficient
                if (v.ttl < self.__timer):
                    self.__size -= v.value + self.__overhead #decrease cache size
                    self.__key_to_node.pop(k)
                    self.__ttl_expirations += 1

            #after removing expired nodes, check if enough space
            if (self.__capa < self.__size + value + self.__overhead): #TODO make more efficient
                ##still not enough space, need to use LFU to evict items until enough space
                #self.__key_to_node.sort(key=lambda node: node.freq)
                #s = sorted(self.__key_to_node.items(), key=lambda x: x.freq, reverse=True)
                s = dict(sorted(self.__key_to_node.items(), key=lambda item: item[1].freq))
                self.__key_to_node = s
                #minimum = min([i.value for i in self.__key_to_node.values()])
                for k, v in list(self.__key_to_node.items()):
                    if (v.freq != 0 and self.__capa/2 >= self.__size): #Remove half of cache with lowest freq   #(self.__capa >= self.__size + value + self.__overhead): #item can now fit
                        return
                    else:
                        #print(f"removing freq {v.freq}")
                        self.__size -= v.value + self.__overhead #decrease cache size
                        self.__key_to_node.pop(k) #remove minimum freq
                        self.__evictions += 1

        #add item
        node = ListNode(key, value, ttl+self.__timer)
        self.__key_to_node[key] = node
        self.__size += value + self.__overhead
        #self.__key_to_node[key].value = value
        #self.__key_to_node[key].ttl = ttl
        self.__insertions += 1
        return

    def getSize(self):
        return self.__size

    def printStats(self):
        print(f"evictions {self.__evictions}")
        print(f"hits {self.__hits}")
        print(f"misses {self.__misses}")
        print(f"expirations {self.__ttl_expirations}")
        print(f"insertions {self.__insertions}")



if __name__ == "__main__":
    start = time.time()
    full_heapsize=1048576000 #1,048,576,000
    sampling_rate = 1000
    heapsize = full_heapsize // sampling_rate
    overhead = 2 * sys.getsizeof(int())
    print(f"overhead {overhead}")
    cache = LFUCache(heapsize, overhead)
    # readfile = open("n.sbin-10000000_items_10_ttl.txt","r")
    fpath = "c:/Users/Gulhan/Desktop/CSE514-CacheSystem-main/mix1_cache.sbin-sampled_1000_items_10_ttl_mix_1.txt"
    readfile = open(fpath,"r")
    filelines = readfile.readlines()
    length = 10000000
    numofinsertions = length/2
    counter =0
    totallen=0
    hits = 0
    misses = 0
    ctr = 0
    print(len(filelines)) #11880217
    for line in filelines:
        ctr+=1
        if (ctr%100000 == 0):
            print(f"progress %{(ctr*100)//len(filelines)}")
            #print(f"hits {hits}")
            #print(f"misses {misses}")
            print(f"miss ratio {misses/(misses+hits)}")
            #print(f"counter {ctr}")
            #print(f"cache capacity {cache.getSize()}")
        data = line.split(" ")
        current_time = int(data[0])
        obj = data[1]
        value = int(data[2])
        totallen+=value
        ttl = int(data[3])
        #print(obj,value,ttl)
        if (cache.get(obj) >= 0):
            #hit
            #print("==HIT")
            hits += 1
        else:
            #miss
            misses+=1
            if (ttl == 10 or ttl == 0):
                ttl = 150
            cache.put(obj, value, current_time, ttl)
        counter += 1
        #if(counter>=10e6):
        #    break
    print(f"misses: {misses}    hits: {hits}")
    print(f"miss ratio {misses/(misses+hits)}")
    '''cache.items_with_timestamp()
    cache.get_hitratio()
    cache.get_missratio()
    cache.get_eviction()
    cache.get_evictionbyttl()
    cache.get_numofitems()
    cache.get_currentcachesize()'''
    print("=cache stats=")
    cache.printStats()
    #print("Total len=",totallen)
    #print("Heap Size=",heapsize)
    end = time.time()

    print(f"time: {end - start} seconds")
