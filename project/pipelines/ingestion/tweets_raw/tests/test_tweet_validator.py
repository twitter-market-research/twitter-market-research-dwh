from project.pipelines.ingestion.tweets_raw.utils.tweet_validator import TweetValidator


class TestTweetValidator:

    def test_valid_tweet_passes_validation(self, valid_tweet):
        """
        This test checks that a valid tweet passes validation.
        Args:
            valid_tweet (dict): A fixture that provides a
             valid tweet dictionary.
        """
        validator = TweetValidator()
        assert validator.is_valid_tweet(valid_tweet) is True

    def test_missing_required_field_fails(self, invalid_tweet_missing_fields):
        """
        This test checks that a tweet missing required fields fails validation.
        Args:
            - invalid_tweet_missing_fields (dict): A fixture that provides
            a tweet dictionary missing required fields.
        """
        validator = TweetValidator()
        assert validator.is_valid_tweet(invalid_tweet_missing_fields) is False

    def test_missing_tweet_id_fails(
        self,
        valid_tweet: dict
    ):
        """
        This test checks that a tweet missing
         the 'tweet_id' field fails validation.
        Args:
           - valid_tweet (dict): A fixture that provides a valid
            tweet dictionary.
        """
        tweet = valid_tweet.copy()
        del tweet["tweet_id"]
        validator = TweetValidator()
        assert validator.is_valid_tweet(tweet) is False

    def test_invalid_lang_fails(
        self,
        valid_tweet
    ):
        """
        This test checks that a tweet with an invalid 'lang'
        field fails validation.
        Args:
           - valid_tweet (dict): A fixture that provides
            a valid tweet dictionary.
        """
        tweet = valid_tweet.copy()
        tweet["lang"] = "en"  # invalid language code
        validator = TweetValidator()
        assert validator.is_valid_tweet(tweet) is False

    def test_get_validation_errors_returns_list(
        self,
        invalid_tweet_missing_fields: dict
    ):
        """
        This test checks that the get_validation_errors method returns a
         list of errors.
        Args:
            - invalid_tweet_missing_fields (dict): A fixture that provides
            a tweet dictionary missing required fields.
        """
        validator = TweetValidator()
        errors = validator.get_validation_errors(invalid_tweet_missing_fields)
        assert isinstance(errors, list)
        assert len(errors) > 0
