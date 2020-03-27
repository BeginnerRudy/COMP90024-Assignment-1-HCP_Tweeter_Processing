from mpi4py import MPI
from TweetReader import TweetReader

#
# comm = MPI.COMM_WORLD
# size = comm.Get_size()
# rank = comm.Get_rank()
#
# if rank == 0:
#     i = 0
#
#     while True:
#         if i >= size-1:
#             break
#         s = comm.recv()
#         print(s)
#         i += 1
# else:
#     print("process", rank, "sent")
#     s = comm.send("done" + str(rank), 0)

tinnyTweetsReader = TweetReader("../data/tinyTwitter.json")
tinnyTweets = tinnyTweetsReader.read_line_skip_header()
for tweet in tinnyTweets:
    print(tweet)
