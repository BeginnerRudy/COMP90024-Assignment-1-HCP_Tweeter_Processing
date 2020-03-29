import json


class TweetReader:
    """
    This class aims to provide a memory efficient way to read tweet JSON files.

    """

    def __init__(self, tweets_filepath, rank, size):
        """

        Args:
            tweets_filepath (): The filepath of the tweet JSON file.
        """
        self.tweets_filepath = tweets_filepath
        self.header = None
        self.size = size
        self.rank = rank
        self.step = size - 1

    def header_info(self):
        """
        This function aims to extract useful information from the header.
        Currently, just return the raw header string, later modify needed.

        Returns:
            The raw header string.
        """
        return self.header

    def read_line_skip_header(self):
        """
        This function would create a generator for parsing the JSON file line by line.
        In this way, we do not need to load the whole file into the RAM, which provides
        nice memory efficiency.

        """
        with open(self.tweets_filepath, encoding='utf-8') as file:
            # store the header
            self.header = file.readline()
            # skip first rank line for each process, to make sure every line get read exactly only once.
            for i, line in enumerate(file):
                if i % self.size == self.rank:
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
