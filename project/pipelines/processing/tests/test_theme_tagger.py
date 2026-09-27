from pathlib import Path

from project.pipelines.processing.utils.theme_tagger import (
    THEME_KEYWORDS,
    LIGUE1_CONTEXT,
    tag_themes,
    build_search_query
)
ENV_DIST = Path(__file__).resolve().parents[4] / "iac" / ".env.dist"


class TestTagThemes:
    """Rule-based theme tagging from a tweet's text and hashtags.

    The catalogue holds four themes: stats/analytics, VAR/refereeing,
    mercato and tactics. A single tweet can match several (or none).
    """

    def test_matches_var_from_text(self) -> None:
        """A free-text mention of VAR tags the ``var`` theme."""
        themes = tag_themes("Encore une decision VAR incomprehensible", [])
        assert themes == ["var"]

    def test_matches_var_from_hashtag_camelcase(self) -> None:
        """A camelCase hashtag (#VAR) still matches the ``var`` theme."""
        themes = tag_themes("Quel match", ["Ligue1", "VAR"])
        assert "var" in themes

    def test_matches_stats_analytics(self) -> None:
        """xG / stats vocabulary tags ``stats_analytics``."""
        themes = tag_themes("Les stats xG montrent la domination", [])
        assert themes == ["stats_analytics"]

    def test_matches_mercato(self) -> None:
        """Transfer vocabulary tags ``mercato``."""
        themes = tag_themes("Gros transfert au mercato d'hiver", [])
        assert themes == ["mercato"]

    def test_matches_tactique(self) -> None:
        """Playing-style vocabulary tags ``tactique``."""
        themes = tag_themes("Un pressing haut et une compo audacieuse", [])
        assert themes == ["tactique"]

    def test_can_match_several_themes(self) -> None:
        """A tweet touching several topics gets every matching theme."""
        text = "Les stats xG apres la VAR, et cette compo tactique"
        assert set(tag_themes(text, [])) == {
            "stats_analytics", "var", "tactique",
        }

    def test_no_theme_returns_empty_list(self) -> None:
        """An off-topic tweet returns an empty list, never None."""
        assert tag_themes("Belle victoire ce soir, bravo les gars", []) == []

    def test_accent_insensitive(self) -> None:
        """Matching ignores accents (métriques == metriques)."""
        assert tag_themes("Les métriques xG parlent", []) == [
            "stats_analytics",
        ]

    def test_handles_none_inputs(self) -> None:
        """None text / hashtags are treated as empty, no crash."""
        assert tag_themes(None, None) == []

    def test_word_boundary_avoids_false_positive(self) -> None:
        """``var`` must not match a substring inside another word."""
        assert tag_themes("Il varie ses passes en permanence", []) == []

    def test_themes_are_deterministic_order(self) -> None:
        """Returned themes follow the declared THEME_KEYWORDS order."""
        text = "VAR puis mercato puis stats"
        assert tag_themes(text, []) == list(
            t for t in THEME_KEYWORDS if t in tag_themes(text, [])
        )

    def test_env_search_keywords_matches_the_catalogue(self) -> None:
        """SEARCH_KEYWORDS doit rester dérivé de THEME_KEYWORDS.

        Leur divergence est ce qui avait fait tomber la pertinence à 0,3 % :
        on collectait sur les clubs et on taggait sur les thèmes.
        """
        expected = build_search_query(LIGUE1_CONTEXT)
        env = ENV_DIST.read_text(encoding="utf-8")
        line = next(l for l in env.splitlines()
                    if l.strip().startswith("SEARCH_KEYWORDS"))
        assert line.split("=", 1)[1].strip() == expected
