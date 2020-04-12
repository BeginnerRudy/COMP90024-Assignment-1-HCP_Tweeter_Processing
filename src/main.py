from mpi4py import MPI
from Utility import TweetReader, lang_codes_to_dict
from collections import defaultdict
import operator
import argparse

import time

def main():
    """
    This function is used to combine all phases together to do the multi-process parallel computing.
    """

    # specify the parser usage
    parser = argparse.ArgumentParser(usage='mpirun -n <number of processes>> python3 <python file> <tweet file path> <lang codes file path>')
    parser.add_argument('twitterPath', type=str)
    parser.add_argument('langcodesPath', type=str)

    # create the parser
    args = parser.parse_args()

    # create a job
    job = Job(args.twitterPath, args.langcodesPath, MPI.COMM_WORLD)

    # execute the job
    job.exec()

class Job:
    """
    This Job class represents a parallel twitter task that we are asked to do.
    """
    def __init__(self, tweets_file_path, lang_codes_file_path, comm):
        self.tweets_file_path = tweets_file_path
        self.lang_codes_file_path = lang_codes_file_path
        self.comm = comm
        self.rank = comm.Get_rank()
        self.size = comm.Get_size()


    def exec(self):
        start_time = time.time()
        tinny_tweets_reader = TweetReader(self.tweets_file_path, self.rank, self.size)
        tinny_tweets = tinny_tweets_reader.read_tweets()

        # phase 1: read, extract and count info, each process works on its own partition of file.
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
            phase1_time = time.time()
            print("---phase 1 %s seconds ---" % (phase1_time - start_time))
            hashtag_final_dict = defaultdict(int)
            for dict_ in hashtag_data:
                for key, value in dict_.items():
                    hashtag_final_dict[key] += value

            lang_final_dict = defaultdict(int)
            for dict_ in lang_data:
                for key, value in dict_.items():
                    lang_final_dict[key] += value

            sorted_hashtag = sorted(hashtag_final_dict.items(), key=operator.itemgetter(1), reverse=True)[:10]
            sorted_lang = sorted(lang_final_dict.items(), key=operator.itemgetter(1), reverse=True)[:10]

            # phase 3: show the final result
            # Print the top 10 tweeted hashtags
            print("Top 10 Trending Hashtags:")
            for i in range(10):
                (hashtag, count) = sorted_hashtag[i]
                print(f'{i+1}. #{hashtag}, {count:,d}')

            # Get a dictionary of language codes to their corresponding languages
            lan_code_dict = lang_codes_to_dict(self.lang_codes_file_path)

            print("\nTop 10 Trending Languages:")
            for i in range(10):
                (lan_code, count) = sorted_lang[i]
                print(f'{i+1}. {lan_code_dict[lan_code]} ({lan_code}), {count:,d}')

            print("---phase 2 %s seconds ---" % (time.time() - phase1_time))



if __name__ == "__main__":
    main()
