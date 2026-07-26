from project.pipelines.ingestion.tweets_raw.utils.key_builder import KeyBuilder


class TestKeyBuilder:

    def test_extracts_club_from_hashtags_psg(
        self,
        valid_tweet: dict
    ) -> None:
        """
        This test checks that the KeyBuilder correctly extracts the club
        name from the hashtags when PSG is present.
        Args:
            - valid_tweet (dict): A fixture that provides
            a valid tweet dictionary.
        """
        tweet = valid_tweet.copy()
        tweet["hashtags"] = ["Ligue1", "PSG"]
        builder = KeyBuilder()
        assert builder.build(tweet) == b"PSG"

    def test_extracts_club_from_hashtags_om(
        self,
        valid_tweet: dict
    ) -> None:
        """
        This test checks that the KeyBuilder correctly extracts the club
        name from the hashtags when OM is present.

        Args:
            - valid_tweet (dict): A fixture that provides a valid tweet
            dictionary.
        """
        tweet = valid_tweet.copy()
        tweet["hashtags"] = ["Ligue1", "OM"]
        builder = KeyBuilder()
        assert builder.build(tweet) == b"OM"

    def test_key_is_bytes(self, valid_tweet: dict):
        """
        This test checks that the key returned by the KeyBuilder
         is of type bytes.
        Args:
            - valid_tweet (dict): A fixture that provides a valid tweet
            dictionary.
        """
        builder = KeyBuilder()
        result = builder.build(valid_tweet)
        assert isinstance(result, bytes)

    def test_detects_club_case_insensitive(self, valid_tweet):
        """
        This test checks that the KeyBuilder detects club names in a
         case-insensitive manner.
        Args:
            - valid_tweet (dict): A fixture that provides a valid tweet
            dictionary.
        """
        tweet = valid_tweet.copy()
        tweet["hashtags"] = ["ligue1", "ogcnice"]
        builder = KeyBuilder()
        assert builder.build(tweet) == b"OGCNICE"

    def test_fallback_ligue1_when_no_club_found(self, valid_tweet):
        """
        This test checks that the KeyBuilder falls back to 'Ligue1' when
        no club name is present in the hashtags.

        Args:
            - valid_tweet (dict): A fixture that provides a valid tweet
            dictionary.
        """
        tweet = valid_tweet.copy()
        tweet["hashtags"] = ["football", "VAR"]
        builder = KeyBuilder()
        assert builder.build(tweet) == b"Ligue1"

    def test_fallback_ligue1_when_empty_hashtags(self, valid_tweet):
        """
        This test checks that the KeyBuilder falls back to 'Ligue1' when
         the hashtags list is empty.
        Args:
            - valid_tweet (dict): A fixture that provides a valid tweet
            dictionary.
        """
        tweet = valid_tweet.copy()
        tweet["hashtags"] = []
        builder = KeyBuilder()
        assert builder.build(tweet) == b"Ligue1"

    def test_fallback_ligue1_when_hashtags_missing(self, valid_tweet):
        """
        This test checks that the KeyBuilder falls back to 'Ligue1' when
         the hashtags key is missing from the tweet.
        Args:
            - valid_tweet (dict): A fixture that provides a valid tweet
            dictionary.
        """
        tweet = valid_tweet.copy()
        del tweet["hashtags"]
        builder = KeyBuilder()
        assert builder.build(tweet) == b"Ligue1"

    def test_first_club_wins_when_multiple(self, valid_tweet):
        tweet = valid_tweet.copy()
        tweet["hashtags"] = ["PSG", "OM", "Ligue1"]  # PSG en premier
        builder = KeyBuilder()
        assert builder.build(tweet) == b"PSG"
