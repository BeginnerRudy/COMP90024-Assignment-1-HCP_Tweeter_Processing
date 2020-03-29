from mpi4py import MPI
from Utility import TweetReader
import numpy as np
import re

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

tinnyTweetsReader = TweetReader("../data/nanoTwitter.json", rank, size)
tinnyTweets = tinnyTweetsReader.read_line_skip_header()

# A = 0
#
# if rank == 0:
#     tinnyTweetsReader = TweetReader("../data/nanoTwitter.json")
#     tinnyTweets = tinnyTweetsReader.read_line_skip_header()
#     worker_id = 1
#     for tweet in tinnyTweets:
#         comm.send(tweet, dest = worker_id)
#         worker_id += 1
#         worker_id %= size
# else:
#     tweet = comm.recv(source=0)
#     print("rank % d : %s" % (rank, tweet['doc']['text']))

# hash-tags, country codes, iso_language_code, lang,

# user is nested
# metadata contains language_code
# entities caontains hashtags

counter = 0
for tweet in tinnyTweets:
    print("rank: %d. Text: %s" % (rank, tweet['doc']['text']))
