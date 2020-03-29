from mpi4py import MPI
from Utility import TweetReader
import numpy as np
from collections import defaultdict
from collections import Counter

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

tinnyTweetsReader = TweetReader("../data/smallTwitter.json", rank, size)
tinnyTweets = tinnyTweetsReader.read_line_skip_header()


# counter = Counter()
counter = defaultdict(int)
for tweet in tinnyTweets:
    ha =  TweetReader.get_hash_tag(tweet['doc']['text'])
    for h in ha:
        counter[h] += 1

import operator
sorted_x = sorted(counter.items(), key=operator.itemgetter(1), reverse=True)
print(sorted_x)
