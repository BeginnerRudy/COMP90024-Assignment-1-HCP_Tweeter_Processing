import json

from mpi4py import MPI
from Utility import TweetReader

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

tinnyTweetsReader = TweetReader("../data/smallTwitter.json", rank, size)
tinnyTweets = tinnyTweetsReader.read_tweets()

index = rank
buffer = []
for tweet in tinnyTweets:
    # the integer string should be 3 char long, padding blank with '0'
    tweet['doc']['text'] = str(index).zfill(8) + tweet['doc']['text']
    buffer.append(tweet)
    index += size

with open("testData/testSmall.json", mode='w') as out:
    out.write('\n')
    for tweet in buffer:
        json.dump(tweet, out)
        out.write('\n')
