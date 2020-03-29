from mpi4py import MPI
from Utility import TweetReader
import numpy as np
from collections import defaultdict
from collections import Counter
import operator

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

tinnyTweetsReader = TweetReader("../data/smallTwitter.json", rank, size)
tinnyTweets = tinnyTweetsReader.read_line_skip_header()

# counter = Counter()
hashtag_counter = defaultdict(int)
lang_counter = defaultdict(int)
for tweet in tinnyTweets:
    ha = TweetReader.get_hash_tag(tweet['doc']['text'])
    for h in ha:
        hashtag_counter[h] += 1
    lang_counter[TweetReader.get_lang_code(tweet)] += 1

sorted_x = sorted(hashtag_counter.items(), key=operator.itemgetter(1), reverse=True)
sorted_lang = sorted(lang_counter.items(), key=operator.itemgetter(1), reverse=True)
print(sorted_x)
print(sorted_lang)
