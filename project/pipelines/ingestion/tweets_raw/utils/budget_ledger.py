import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

ENV_LEDGER_PATH = "BUDGET_LEDGER_PATH"


class BudgetLedger:
    """
        A class to manage budget Ledger operations,
        including reading and writing budget data to a JSON file.

        Parameters
        ----------
        path : str
            Fichier JSON portant le ledger .
        limit : float, optimal
            Budget limit in dollors (default 25.0).
    """

    @classmethod
    def from_env(cls, limit: float = 25.0) -> Optional["BudgetLedger"]:
        """
        Create the ledger  if  "BUDGET_LEDGER_PATH" is set 
        in the environment variables.

        Returns
        -------
        BudgetLedger or None
          ''' None ''' if the environment variable is not set,
          otherwise a BudgetLedger instance.
        """
        path = os.environ.get(ENV_LEDGER_PATH)
        return cls(Path(path), limit=limit) if path else None

    def __init__(
        self,
        path: str,
        limit: float = 25.0
    ):
        self.path = Path(path)
        self.limit = limit
        self._state = self.load_state()

    def load_state(self) -> Dict[str, Any]:
        """
        Read the bufget ledger, and return the state as a dictionary.
        """
        empty_state = {"tweets_fetched": 0, "spent": 0.0, "runs": 0}
        try:
            with self.path.open(encoding="utf-8") as handle:
                return json.load(handle)
        except FileNotFoundError:
            return empty_state 
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"ledger unreadable ({e}) - reset")
            return empty_state

    @property
    def spent(self) -> float:
        """
        Total spent in dollors.
        """
        spent_budget = float(self._state.get("spent", 0.0))
        return spent_budget

    @property
    def tweets_fetched(self):
        """
        Cumulative number of billed tweets
        """
        return int(self._state.get("tweets_fetched", 0))

    @property
    def remaining(self) -> float:
        """
        Left Budget in dollors, never negative.
        """
        budget = max(0, self.limit - self.spent)
        return budget

    def record(
            self,
            tweets: int,
            cost_per_tweet: float
    ) -> None:
        """
        Record the number of tweets fetched and the cost per tweet
        and write it to the disk

        Parameters
        ----------
        tweets : int
            Number of tweets fetched.
        cost_per_tweet : float
            Cost per tweet in dollars.
        """
        if tweets <= 0:
            return
        self._state["tweets_fetched"] = (
            int(self._state.get("tweets_fetched", 0)) + tweets
        )

        self._state["runs"] = int(self._state.get("runs", 0)) + 1

        self._state["spent"] = round(
            self.spent + tweets * cost_per_tweet, 4
        )

        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("w", encoding="utf-8") as handles:
                json.dump(self._state, handles, indent=2)
        except OSError as exception:
            logger.error(f"Failed to write ledger ({exception})")
