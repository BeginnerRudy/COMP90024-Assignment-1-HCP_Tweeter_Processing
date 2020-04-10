import json
import re
import os
import csv
from collections import defaultdict


class TweetReader:
    """
    This class aims to provide a memory efficient way to read tweet JSON files.

    """

    def __init__(self, tweets_filepath, rank, size):
        """

        Args:
            tweets_filepath (): The filepath of the tweet JSON file.
        """
        self.tweets_file = open(tweets_filepath, 'r')
        self.size = size
        self.rank = rank

        # partition the  file, get partition size in bytes
        file_size = os.fstat(self.tweets_file.fileno()).st_size  # The file size in bytes
        partition_size = file_size / size

        # partition file based on rank roughly
        rough_start = int(partition_size * rank)
        rough_end = int(partition_size * (rank + 1))

        # align to the next newline, since we could simply ignore the 1st line of JSON file
        self.file_start = self.go_to_next_line(rough_start)
        self.file_end = self.go_to_next_line(rough_end)

        # set file to the current start position
        self.tweets_file.seek(self.file_start)

    def go_to_next_line(self, offset):
        self.tweets_file.seek(offset)  # set file to offset
        # keep read util reach a newline or EOF
        while self.tweets_file.read(1) not in ['', '\n']:
            pass
        return self.tweets_file.tell()  # return the current position of the file

    def read_tweets(self):
        """
        This function would create a generator for parsing the JSON file line by line.
        In this way, we do not need to load the whole file into the RAM, which provides
        nice memory efficiency.

        """
        # skip first rank line for each process, to make sure every line get read exactly only once.
        while self.tweets_file.tell() < self.file_end:
            line = self.tweets_file.readline()
            try:
                # Truncate the valid JSON string for all lines except the last 2 lines
                yield json.loads(line[:-2])
            except json.decoder.JSONDecodeError:
                try:
                    # Truncate the valid JSON string for all lines except the 2nd last line
                    yield json.loads(line)
                except json.decoder.JSONDecodeError:
                    # Ignore the last line, since it does not contain any data.
                    pass

        self.tweets_file.close()

    @staticmethod
    def get_hash_tag(string):
        """
        This function aims to extract and lower casing hashtags from the given string and return as a list.
        Note:  A hashtag is a string following a # that ends with a space or any
            punctuation (except for underscore _ )

        Args:
            string ([str]): The string given to extract

        Returns:
            A list of lower cased hashtags.
        """
        return [x.lower() for x in re.findall(r"#([a-zA-Z0-9_]+)", string)]

    @staticmethod
    def get_lang_code(tweet):
        return tweet['doc']['metadata']['iso_language_code']

def lang_codes_to_dict(filepath):
    """
    Convert a csv file of language codes to a dictionary

    Args:
        filepath: The filepath to the csv file

    Returns:
        A dictionary where key is a language code and value is its full name
    """
    reader = csv.DictReader(open(filepath, 'r'))
    lang_dict = defaultdict(str)

    for line in reader:
        if 'alpha2' in line:
            lang_dict[line['alpha2']] = line['English']
        if 'alpha3-b' in line:
            lang_dict[line['alpha3-b']] = line['English']
    
    return lang_dict