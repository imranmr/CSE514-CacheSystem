#!/usr/bin/env python3

import argparse
import struct


def change_get_to_set(trace_path, default_ttl):
    #default_ttl_list: 86400:0.65,1296000:0.27,43200:0.07
    """
    because the cache is cold (no item inside at start time),
    get/gets/cas/replace/incr/decr/append/prepend/delete
    has no effect and trace_replay will give false alarm (not found in cache)

    so this func processes the trace, change the first
    get/gets/cas/replace/incr/decr/append/prepend/delete of each item to set

    :param trace_path:
    :return:
    """
    s = struct.Struct("<IQII")
    seen_obj = set()
    cached_obj = set()
    start_ts, end_ts = -1, -1
    n_req = 0

    ifile = open(trace_path, "rb")
    # ofile = open(trace_path + ".processed", "wb")
    # ofile = open(trace_path + "-100000000_items_10_ttl.txt", "w")
    ofile = open(trace_path + "-temp.txt", "w")
    r = ifile.read(s.size)
    # r=ifile.readline()[-1]
    while r:
        n_req += 1
        ts, obj, kv_len, op_ttl = s.unpack(r)
        r = ifile.read(s.size)

        if start_ts == -1:
            start_ts = ts

        op = (op_ttl >> 24) & (0x00000100 - 1)
        ttl = op_ttl & (0x01000000 - 1)
        key_len = (kv_len >> 22) & (0x00000400 - 1)
        val_len = kv_len & (0x00400000 - 1)

        # op index (index starts from 1):
        # get, gets, set, add, cas, replace, append, prepend, delete, incr, decr
        if obj not in seen_obj and op != 3 and op != 4:
            op = 3
            if ttl == 0:
                ttl = default_ttl
        print("TTL=",ttl,"kvlen=",kv_len,"key_len",key_len)
        op_ttl_new = (op << 24) | (ttl & (0x01000000 - 1))

        if key_len == 0:
            print("trace contains request of key size 0, object id {}".
                  format(obj))

        # if 3 <= op <= 6:
        #     cached_obj.add(obj)
        #
        # if op == 9:
        #     if obj not in cached_obj:
        #         print("failed delete {}".format(obj))
        #     else:
        #         cached_obj.remove(obj)
        # print(ts, obj, kv_len, op_ttl_new)
        #(epoch time, obj, length of data, time to live
        # ofile.write(s.pack(ts, obj, kv_len, op_ttl_new))
        stringval = str(ts)+" "+ str(obj) +" "+ str(key_len) +" "+str(op_ttl_new)+"\n"
        ofile.write(stringval)
        seen_obj.add(obj)
        print(ttl,op_ttl_new)
        # if(n_req==1):
        #
        #     break
        if(n_req==100000000):

            break
    end_ts = ts

    ifile.close()
    ofile.close()
    print("time range {}-{} ({} sec) total {} obj".format(
        start_ts, end_ts, end_ts - start_ts, len(seen_obj)))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--func", help="the function you want to run", type=str,
                    default="change_get_to_set")
    ap.add_argument("trace", help="the path to the trace", type=str)
    ap.add_argument("default_ttl", help="the default ttl", type=int)

    args = ap.parse_args()

    globals()[args.func](args.trace, args.default_ttl)

#command run: python3 trace_conv.py n.sbin 100
