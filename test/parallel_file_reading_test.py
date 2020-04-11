from mpi4py import MPI
from Utility import TweetReader
import numpy as np
import re

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

tinnyTweetsReader = TweetReader("testData/testSmall.json", rank, size)
tinnyTweets = tinnyTweetsReader.read_tweets()

# let each processes read its own tweet's index
tweet_indies = []
for tweet in tinnyTweets:
    # print("rank: %d. Text: %s" % (rank, tweet['doc']['text'][:3]))
    tweet_indies.append(int(tweet['doc']['text'][:8]))

# gather tweet's index
data = comm.gather(tweet_indies, root=0)
# the root process does the final check
if rank == 0:
    data_ = [j for sub in data for j in sub]
    data_ = sorted(data_)
    print(data_[-1])
    print(list(range(data_[-1]+1)) == data_)
