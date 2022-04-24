# CSE514-CacheSystem
This Cache System requires the use of a specific dataset (traces) provided by Twitter.
The dataset is listed under the following link:
https://github.com/Thesys-lab/Segcache

The 5 traces can be found with the provided link from the github repository or use the following link below:
https://ftp.pdl.cmu.edu/pub/datasets/twemcacheWorkload/nsdi21_binary/
 
For the best set of data, download the "mix1_cache.sbin.zst" file listed in the directory of the link above.
Once downloaded, use the following command to extract the data:
  zstd -d mix1_cache.sbin.zst

Now that you have the uncompressed file, use the trace_conv_sample.py file to convert the binary data into a readable format.
Please enter the .sbin file followed by TTL to run the conversion.
Example: "python3 trace_conv_sample.py mix1_cache.sbin 100"

Once you have the .txt file output, you can run the following two tests to check LRU and LFU caches as well as their performance.

Simply run:
python3 LFUCache_TTL.py
    or
python3 LRUCache_TTL_main.py
  
  
