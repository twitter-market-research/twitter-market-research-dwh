from project.pipelines.processing.utils.theme_tagger import (
    THEME_KEYWORDS,
    tag_themes,
)


class TestTagThemes:
    """Rule-based theme tagging from a tweet's text and hashtags.

    Themes come from the KPI spec: video analysis, stats/analytics and
    VAR/refereeing. A single tweet can match several themes (or none).
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

    def test_matches_video_analysis(self) -> None:
        """Video-analysis vocabulary tags ``analyse_video``."""
        themes = tag_themes("Belle analyse video du montage tactique", [])
        assert themes == ["analyse_video"]

    def test_can_match_several_themes(self) -> None:
        """A tweet touching two topics gets both themes."""
        themes = tag_themes("Analyse video des stats xG apres la VAR", [])
        assert set(themes) == {"analyse_video", "stats_analytics", "var"}

    def test_no_theme_returns_empty_list(self) -> None:
        """An off-topic tweet returns an empty list, never None."""
        assert tag_themes("Belle victoire ce soir, bravo les gars", []) == []

    def test_accent_insensitive(self) -> None:
        """Matching ignores accents (analyse vidéo == analyse video)."""
        assert tag_themes("Superbe analyse vidéo", []) == ["analyse_video"]

    def test_handles_none_inputs(self) -> None:
        """None text / hashtags are treated as empty, no crash."""
        assert tag_themes(None, None) == []

    def test_word_boundary_avoids_false_positive(self) -> None:
        """``var`` must not match a substring inside another word."""
        assert tag_themes("Il varie ses passes en permanence", []) == []

    def test_themes_are_deterministic_order(self) -> None:
        """Returned themes follow the declared THEME_KEYWORDS order."""
        text = "VAR puis analyse video puis stats"
        assert tag_themes(text, []) == list(
            t for t in THEME_KEYWORDS if t in tag_themes(text, [])
        )
