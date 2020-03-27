import json


class TweetReader:
    """
    This class aims to provide a memory efficient way to read tweet JSON files.

    """

    def __init__(self, tweets_filepath):
        """

        Args:
            tweets_filepath (): The filepath of the tweet JSON file.
        """
        self.tweets_filepath = tweets_filepath
        self.header = None

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
            for line in file:
                data = self.to_json(line)
                yield data

    @staticmethod
    def to_json(string: str):
        """
        This function takes a json string as input and return the parsed json object.
        There are 2 assumptions here:
            1. For the last line l, l[:-3] is a valid JSON string.
            2. For other lines p, p[:-2] is a valid JSON string.

        Args:
            string (): The Json string to be parsed.

        Returns:
            The parsed json object.
        """
        # not last line
        try:
            # Truncate the valid JSON string for all lines except the last line
            return json.loads(string[:-2])

        # last line
        except json.decoder.JSONDecodeError:
            # Truncate the valid JSON string for the last line
            return json.loads(string[:-3])
