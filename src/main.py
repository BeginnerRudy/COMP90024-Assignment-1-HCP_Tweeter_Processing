from mpi4py import MPI
from Utility import TweetReader
import numpy as np
from collections import defaultdict
from collections import Counter
import operator
import argparse
from heapq import nlargest
from timeit import default_timer as timer


def main():
    """
    This function is used to combine all single steps together to do the multi-process parallel computing.
    """

    # specify the parser usage
    parser = argparse.ArgumentParser(usage='mpirun -n <number of processes>> python3 <python file> <tweet file path>')
    parser.add_argument('twitterPath', type=str)

    # create the parser
    args = parser.parse_args()

    # create a job
    job = Job(args.twitterPath, MPI.COMM_WORLD)

    # execute the job
    job.exec()


class Job:

    def __init__(self, tweets_file_path, comm):
        self.tweets_file_path = tweets_file_path
        self.comm = comm
        self.rank = comm.Get_rank()
        self.size = comm.Get_size()

    def exec(self):

        tinny_tweets_reader = TweetReader(self.tweets_file_path, self.rank, self.size)
        tinny_tweets = tinny_tweets_reader.read_tweets()

        # phase 1: read, extract and count info
        # counter = Counter()
        hashtag_counter = defaultdict(int)
        lang_counter = defaultdict(int)
        count = 0
        for tweet in tinny_tweets:
            ha = TweetReader.get_hash_tag(tweet['doc']['text'])
            for h in ha:
                hashtag_counter[h] += 1
            lang_counter[TweetReader.get_lang_code(tweet)] += 1
            count += 1

        hashtag_data = self.comm.gather(hashtag_counter, root=0)
        lang_data = self.comm.gather(lang_counter, root=0)

        # phase 2: parallel combine the result
        if self.rank == 0:
            start = timer()
            hashtag_final_dict = defaultdict(int)
            for dict_ in hashtag_data:
                for key, value in dict_.items():
                    hashtag_final_dict[key] += value

            lang_final_dict = defaultdict(int)
            for dict_ in lang_data:
                for key, value in dict_.items():
                    lang_final_dict[key] += value
            sorted_hashtag = sorted(hashtag_final_dict.items(), key=operator.itemgetter(1), reverse=True)
            sorted_lang = sorted(lang_final_dict.items(), key=operator.itemgetter(1), reverse=True)
            # # phase 3: show the final result
            print(sorted_hashtag[:10])
            print(sorted_lang[:10])
            end = timer()
            print(start)
            print("Total summarizing time: %lf " % (end - start))
        # print("Total number of Tweets: %d" % count)


if __name__ == "__main__":
    main()
