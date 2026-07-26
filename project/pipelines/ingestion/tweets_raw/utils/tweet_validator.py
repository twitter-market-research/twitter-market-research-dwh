
from dataclasses import dataclass, field
from typing import List
ALLOWED_LANGS = ["fr"]
REQUIRED_FIELDS = [
    "id",
    "text", 
    "created_at",
    "lang",
    "author_id",
    "public_metrics",
]


@dataclass
class ValidationResult:
    is_valid: bool
    errors: List[str] = field(default_factory=list)


class TweetValidator:

    def validate(self, tweet: dict) -> ValidationResult:
        """This function validates a tweet dictionary against
         required fields and rules.
        Args:
            tweet (dict): The tweet dictionary to validate.
        Returns:
            ValidationResult: The validation result.
        """
        errors = []  # ← initialise la liste locale
        errors.extend(self._check_required_fields(tweet))
        errors.extend(self._check_lang(tweet))
        errors.extend(self._check_counts(tweet))
        errors.extend(self._check_types(tweet))
        return ValidationResult(is_valid=len(errors) == 0, errors=errors)

    def is_valid_tweet(self, tweet: dict) -> bool:
        """This function checks if a tweet is valid.
        Args:
            tweet (dict): The tweet dictionary to check.
        Returns:
            bool: True if the tweet is valid, False otherwise.
        """
        return self.validate(tweet).is_valid

    def get_validation_errors(self, tweet: dict) -> List[str]:
        return self.validate(tweet).errors

    def _check_required_fields(self, tweet: dict) -> List[str]:
        """This function checks for the presence of
         required fields in the tweet.
        Args:
            tweet (dict): The tweet dictionary to check.
        Returns:
            List[str]: A list of error messages for missing fields.
        """
        errors = []
        for f in REQUIRED_FIELDS:
            if f not in tweet:
                errors.append(f"Champ manquant : {f}")
        # Vérifie les sous-champs de public_metrics
        metrics = tweet.get("public_metrics", {})
        for m in ["like_count", "retweet_count"]:
            if m not in metrics:
                errors.append(f"Champ manquant : public_metrics.{m}")
        return errors

    def _check_lang(self, tweet: dict) -> List[str]:
        """This function checks that the 'lang' field
         is one of the allowed languages.
        Args:
            tweet (dict): The tweet dictionary to check.
        Returns:
            List[str]: A list of error messages for invalid language fields.
        """
        if "lang" in tweet and tweet["lang"] not in ALLOWED_LANGS:
            lang = tweet['lang']
            msg = f"Langue non autorisée : {lang} (attendu : {ALLOWED_LANGS})"
            return [msg]
        return []

    def _check_counts(self, tweet: dict) -> List[str]:
        """This function checks that count fields are non-negative integers.
        Args:
            tweet (dict): The tweet dictionary to check.
        Returns:
            List[str]: A list of error messages for invalid count fields.
        """
        errors = []
        metrics = tweet.get("public_metrics", {})
        for count_field in ("like_count", "retweet_count"):
            if count_field in metrics and metrics[count_field] < 0:
                errors.append(f"{count_field} ne peut pas être négatif")
        return errors

    def _check_types(self, tweet: dict) -> List[str]:
        """This function checks that certain fields have the correct types.
        Args:
            tweet (dict): The tweet dictionary to check.
        Returns:
            List[str]: A list of error messages for invalid field types.
        """
        errors = []
        entities = tweet.get("entities", {})
        hashtags = entities.get("hashtags", [])
        if not isinstance(hashtags, list):
            errors.append("hashtags doit être une liste")
        if "id" in tweet and not isinstance(tweet["id"], str):
            errors.append("id doit être une chaîne de caractères")
        return errors
