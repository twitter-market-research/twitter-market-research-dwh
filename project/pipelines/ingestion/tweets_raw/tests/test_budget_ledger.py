from project.pipelines.ingestion.tweets_raw.utils.budget_ledger import (
    BudgetLedger,
)


class TestBudgetLedger:
    """
     Class for testing the functions in the BudgetLedger class.
    """

    def test_starts_empty_when_file_absent(self, tmp_path) -> None:
        """
        Test that the ledger starts empty when the file is absent.
        """
        ledger = BudgetLedger(str(tmp_path / "budget.json"))
        assert ledger.spent == 0.0
        assert ledger.remaining == 25.0

    def test_record_accumulates(self, tmp_path) -> None:
        """
        Test that the record method accumulates the number of tweets and spent budget.
        """

        path = str(tmp_path/ "budget.json")
        BudgetLedger(path).record(40, 0.005)
        BudgetLedger(path).record(60, 0.005)

        assert BudgetLedger(path).spent == 0.5

    def test_remaining_never_negative(self, tmp_path) -> None:
        """
        Test that the remaining budget goes negative.
        """
        ledger = BudgetLedger(str(tmp_path / "budget.json"), limit=0.10)
        ledger.record(100, 0.005)

        assert ledger.remaining == 0.0

    def test_corrupt_file_resets_instead_of_crashing(self, tmp_path) -> None:
        """
        A JSON broke never have to stop the ingestion
        """
        path = tmp_path / "budget.json"
        path.write_text("{ pas du json}", encoding="utf-8")

        assert BudgetLedger(str(path)).spent == 0.0

    def test_from_env_returns_none_without_variable(self, monkeypatch) -> None:
        """
        Without the varaiable, the cumuled ledger is deactivated
        """
        monkeypatch.delenv("BUDGET_LEDGER_PATH", raising=False)

        assert BudgetLedger.from_env() is None

    def test_from_env_builds_ledger(
        self,
        tmp_path,
        monkeypatch
    ) -> None:
        monkeypatch.setenv("BUDGET_LEDGER_PATH", str(tmp_path / "b.json"))

        assert isinstance(BudgetLedger.from_env(), BudgetLedger)

    def test_tweets_fetched_accumulates(self, tmp_path) -> None:
        """The tweet counter accumulates instead of holding the last batch."""
        path = str(tmp_path / "budget.json")
        BudgetLedger(path).record(40, 0.005)
        BudgetLedger(path).record(60, 0.005)

        assert BudgetLedger(path).tweets_fetched == 100









