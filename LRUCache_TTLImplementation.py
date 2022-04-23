import time
from threading import RLock
import sys
from typing import Any, Union
import tqdm
import random

try:
    from collections import OrderedDict
except ImportError:
    # Python < 2.7
    from ordereddict import OrderedDict


class ExpiringDict(OrderedDict):
    def __init__(self, max_len, max_age_seconds, items=None):
        # type: (Union[int, None], Union[float, None], Union[None,dict,OrderedDict,ExpiringDict]) -> None

        if not self.__is_instance_of_expiring_dict(items):
            self.__assertions(max_len, max_age_seconds)

        OrderedDict.__init__(self)
        self.max_len = max_len
        self.current_len = 0 #update current_len on every entry
        self.max_age = max_age_seconds
        self.lock = RLock()
        self.hit = 0
        self.totalrequest = 0
        self.num_evicted = 0
        self.evictionbyttl=0

        if sys.version_info >= (3, 5):
            self._safe_keys = lambda: list(self.keys())
        else:
            self._safe_keys = self.keys

        if items is not None:
            if self.__is_instance_of_expiring_dict(items):
                self.__copy_expiring_dict(max_len, max_age_seconds, items)
            elif self.__is_instance_of_dict(items):
                self.__copy_dict(items)
            elif self.__is_reduced_result(items):
                self.__copy_reduced_result(items)

            else:
                raise ValueError('can not unpack items')

    def __contains__(self, key):
        """ Return True if the dict has a key, else return False. """
        self.totalrequest += 1
        try:
            with self.lock:
                item = OrderedDict.__getitem__(self, key)
                if time.time() - item[1] < self.max_age:
                    self.hit+=1
                    return True
                else:
                    del self[key]
                    self.num_evicted+=1
                    self.evictionbyttl+=1
        except KeyError:
            pass
        return False

    def __getitem__(self, key, with_age=False):
        """ Return the item of the dict.
        Raises a KeyError if key is not in the map.
        """
        self.totalrequest+=1
        with self.lock:
            item = OrderedDict.__getitem__(self, key)
            item_age = time.time() - item[1]
            self.hit+=1
            if item_age < self.max_age:
                if with_age:
                    return item[0], item_age
                else:
                    return item[0]
            else:
                del self[key]
                self.num_evicted += 1
                self.evictionbyttl += 1
                raise KeyError(key)

    def __setitem__(self, key, value, set_time=None):
        """ Set d[key] to value. """
        with self.lock:
            if len(self) == self.max_len or (value[0]+self.current_len >= self.max_len):
                if key in self:
                    del self[key]
                    self.num_evicted += 1
                    self.totalrequest += 1
                    self.hit+=1
                    item = OrderedDict.__getitem__(self, key)
                    print("currentsize=",self.current_len,"newlen=",value[0],"item=",item)
                    self.current_len-= int(item[1][0])
                else:
                    try:
                        item = self.popitem(last=False)
                        self.current_len -= int(item[1][0])
                        print("currentsize=", self.current_len, "newlen=", value[0])
                        self.num_evicted += 1
                        self.totalrequest += 1
                    except KeyError:
                        pass
            if set_time is None:
                set_time = time.time()
            while(value[0]+self.current_len>=self.max_len):
                try:
                    item = self.popitem(last=False)
                    print(item)
                    self.current_len -= int(item[1][0])
                    print("currentsize=", self.current_len, "newlen=", value[0],"check size",item[1][0])
                    self.num_evicted += 1
                    self.totalrequest += 1
                except KeyError:
                    pass
            OrderedDict.__setitem__(self, key, (value, set_time))
            self.current_len+=value[0]

    def pop(self, key, default=None):
        """ Get item from the dict and remove it.
        Return default if expired or does not exist. Never raise KeyError.
        """
        self.num_evicted +=1
        with self.lock:
            try:
                item = OrderedDict.__getitem__(self, key)
                del self[key]
                self.num_evicted += 1
                return item[0]
            except KeyError:
                return default

    def ttl(self, key):
        """ Return TTL of the `key` (in seconds).
        Returns None for non-existent or expired keys.
        """
        key_value, key_age = self.get(key, with_age=True)  # type: Any, Union[None, float]
        if key_age:
            key_ttl = self.max_age - key_age
            if key_ttl > 0:
                return key_ttl
        return None

    def get(self, key, default=None, with_age=False):
        """ Return the value for key if key is in the dictionary, else default. """
        try:
            return self.__getitem__(key, with_age)
        except KeyError:
            if with_age:
                return default, None
            else:
                return default

    def items(self):
        """ Return a copy of the dictionary's list of (key, value) pairs. """
        r = []
        for key in self._safe_keys():
            try:
                r.append((key, self[key]))
            except KeyError:
                pass
        return r

    def items_with_timestamp(self):
        """ Return a copy of the dictionary's list of (key, value, timestamp) triples. """
        r = []
        for key in self._safe_keys():
            try:
                r.append((key, OrderedDict.__getitem__(self, key)))
            except KeyError:
                pass
        return r

    def values(self):
        """ Return a copy of the dictionary's list of values.
        See the note for dict.items(). """
        r = []
        for key in self._safe_keys():
            try:
                r.append(self[key])
            except KeyError:
                pass
        return r

    def __reduce__(self):
        reduced = self.__class__, (self.max_len, self.max_age, ('reduce_result', self.items_with_timestamp()))
        return reduced

    def __assertions(self, max_len, max_age_seconds):
        self.__assert_max_len(max_len)
        self.__assert_max_age_seconds(max_age_seconds)

    @staticmethod
    def __assert_max_len(max_len):
        assert max_len >= 1

    @staticmethod
    def __assert_max_age_seconds(max_age_seconds):
        assert max_age_seconds >= 0

    @staticmethod
    def __is_reduced_result(items):
        if len(items) == 2 and items[0] == 'reduce_result':
            return True
        return False

    @staticmethod
    def __is_instance_of_expiring_dict(items):
        if items is not None:
            if isinstance(items, ExpiringDict):
                return True
        return False

    @staticmethod
    def __is_instance_of_dict(items):
        if isinstance(items, dict):
            return True
        return False


    def get_hitratio(self):
        if (self.totalrequest != 0):
            print("Hit Ratio:", 1.00 * self.hit / self.totalrequest)

    def get_eviction(self):
        print("Total Evictions:",self.num_evicted)

    def get_evictionbyttl(self):
        print("Total Evictions by TTL:",self.evictionbyttl)

    def get_missratio(self):
        if(self.totalrequest!=0):
            print("Miss Ratio:", 1 - 1.00 * self.hit / self.totalrequest)

    def get_numofitems(self):
        print("Number of items in dictionary:",len(self))

    def get_currentcachesize(self):
        print("Current Cache Size=",self.current_len,((self.max_len-self.current_len)/self.max_len) * 100,"% Free Space Left")

# numofinsertions = 10000000
# heapsize = 10000000
#max_age_seconds=TTL
# cache = ExpiringDict(max_len=heapsize,max_age_seconds=30)
#
# for i in tqdm.tqdm(range(numofinsertions)):#Inserting data into cache
#     randnum = random.randrange(start=1,stop=numofinsertions,step=1)
#     cache[randnum]=randnum
#
# for i in tqdm.tqdm(range(numofinsertions)):#Inserting data into cache
#     randnum = random.randrange(start=1,stop=numofinsertions,step=1)
#     cache.get(randnum)
#
#
# # for i in tqdm.tqdm(range(heapsize)):
# #     cache[i] = i
# #
# # for i in tqdm.tqdm(range(heapsize)):
# #     cache.get(i)
#
# cache.get_hitratio()
# cache.get_missratio()
# cache.get_eviction()
# cache.get_evictionbyttl()
# cache.get_sizeofdict()

#Read data into dictionary, need to consider the length of data as well when inserting

#Data from Trace:
# (epoch time, obj, length of data, new time to live)
# 1585440000 11606970 104857622 50331658
# 1585440000 2814533 104857627 50331658

#Total 1000000 data points, anymore will take too long
#500000 for insertion
#500000 for get searches
#Ignore Time start, key = obj, value = length of data, time = new ttl
start = time.time()
full_heapsize=1048576000 #1,048,576,000
heapsize = 10000000
cache = ExpiringDict(max_len=full_heapsize,max_age_seconds=30)
readfile = open("n.sbin-10000000_items_10_ttl.txt","r")
# readfile = open("mix1_cache.sbin-sampled_1000_items_10_ttl_mix.txt","r")
# readfile = open("mix1_cache.sbin-sampled_1000_items_100_ttl_mix_3.txt","r")
filelines = readfile.readlines()
length = 10000000
numofinsertions = length/2
counter =0
totallen=0
for line in filelines:
    data = line.split(" ")
    obj = int(data[1])
    key_len = int(data[2])
    # key_len = (key_len >> 22) & (0x00000400 - 1)
    totallen+=key_len
    ttl = int(data[3])
    # print(key_len)
    # print(obj,value,ttl)

    if(counter<numofinsertions):
        cache[obj] = (key_len,ttl)
    else:
        cache.get(obj)
    counter += 1
    # if(counter==11):
    #     break
print("totallen=",totallen,"averagesize=",totallen/counter,"Total Items=",counter)
cache.items_with_timestamp()
cache.get_hitratio()
cache.get_missratio()
cache.get_eviction()
cache.get_evictionbyttl()
cache.get_numofitems()
cache.get_currentcachesize()
print("Total len=",totallen)
print("Heap Size=",full_heapsize)
end = time.time()

print(end - start)

