from mpi4py import MPI
from Utility import TweetReader
import numpy as np
from collections import defaultdict
from collections import Counter
import operator
import argparse


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
        tinny_tweets = tinny_tweets_reader.read_line_skip_header()

        # phase 1: read, extract and count info
        # counter = Counter()
        # hashtag_counter = defaultdict(int)
        # lang_counter = defaultdict(int)
        count = 0
        for tweet in tinny_tweets:
            # ha = TweetReader.get_hash_tag(tweet['doc']['text'])
            # for h in ha:
            #     hashtag_counter[h] += 1
            # lang_counter[TweetReader.get_lang_code(tweet)] += 1
            count += 1

        # # phase 2: parallel combine the result
        # sorted_x = sorted(hashtag_counter.items(), key=operator.itemgetter(1), reverse=True)
        # sorted_lang = sorted(lang_counter.items(), key=operator.itemgetter(1), reverse=True)
        #
        # # phase 3: show the final result
        # print(sorted_x[:10])
        # print(sorted_lang[:10])
        print("Total number of Tweets: %d" % count)


if __name__ == "__main__":
    main()
