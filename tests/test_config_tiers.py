"""Unit tests for conviction-tier persistence and the config version hash.

The /logic tier controls were dead in three independent ways:
  1. templates/logic.html posted `conviction_value_over_5m_pts` while
     app.logic_save declared `conviction_value_5m_pts`. FastAPI drops unmatched
     form fields silently, so every tier param was always None.
  2. logic_save wrote the values into conviction_flags under a `tier_pts_`
     prefix, which load_config never read.
  3. The /logic GET passed cfg.CONVICTION_TIERS, the raw module constant, so
     saved values would not display even once persistence worked.

The first test below is the regression guard for (1): it compares the form field
names the template generates against the route's actual signature.
"""
from __future__ import annotations

import inspect
import json

import pytest

import app
import config as cfg


class TestFieldNameContract:
    def test_template_field_names_match_the_route_signature(self):
        """This is the assertion whose absence let the tier controls be dead."""
        params = set(inspect.signature(app.logic_save).parameters)
        expected = {f"conviction_{label}_pts" for label in cfg.conviction_tier_labels()}
        missing = expected - params
        assert not missing, f"logic_save is missing form params for: {sorted(missing)}"

    def test_labels_cover_every_declared_tier(self):
        labels = cfg.conviction_tier_labels()
        assert len(labels) == sum(len(t) for t in cfg.CONVICTION_TIERS.values())
        assert len(set(labels)) == len(labels), "tier labels must be unique"

    def test_template_still_derives_names_from_the_label(self):
        """The contract test above compares config labels to route params. That
        only proves the two ends agree if the template really builds its input
        names from the label, so pin that too."""
        from pathlib import Path

        html = Path(cfg.BASE_DIR / "templates" / "logic.html").read_text(encoding="utf-8")
        assert 'name="conviction_{{ label }}_pts"' in html

    def test_no_stale_tier_pts_params_remain(self):
        """The old names must be gone, not merely joined by the new ones."""
        params = set(inspect.signature(app.logic_save).parameters)
        assert "conviction_value_5m_pts" not in params
        assert "conviction_pct_50_pts" not in params


class TestTierRoundTrip:
    @pytest.fixture
    def overrides(self, tmp_path, monkeypatch):
        path = tmp_path / "config_overrides.json"
        monkeypatch.setattr(cfg, "OVERRIDES_PATH", str(path))
        return path

    def test_saved_points_are_applied_to_the_tier_structure(self, overrides):
        cfg.save_overrides({}, {}, conviction_tiers={"value_over_5m": 7})
        loaded = cfg.load_config()
        value_tiers = loaded["conviction_tiers"]["value"]
        by_label = {label: points for (_, points, label) in value_tiers}
        assert by_label["value_over_5m"] == 7
        assert by_label["value_over_1m"] == 2, "untouched tiers keep their defaults"

    def test_thresholds_and_order_are_preserved(self, overrides):
        """Thresholds are code, not config. The descending-order invariant that
        the scorer relies on must survive a save/load cycle."""
        cfg.save_overrides({}, {}, conviction_tiers={"value_over_250k": 9})
        tiers = cfg.load_config()["conviction_tiers"]["value"]
        thresholds = [t for (t, _, _) in tiers]
        assert thresholds == sorted(thresholds, reverse=True)
        assert thresholds == [5_000_000, 1_000_000, 250_000]

    def test_tier_entries_stay_three_item_unpackable(self, overrides):
        """templates/logic.html does `{% for (threshold, points, label) in ... %}`."""
        cfg.save_overrides({}, {}, conviction_tiers={"pct_over_50": 1})
        for group_tiers in cfg.load_config()["conviction_tiers"].values():
            for entry in group_tiers:
                threshold, points, label = entry
                assert isinstance(label, str)
                assert isinstance(points, int)

    def test_tiers_persist_under_their_own_section(self, overrides):
        cfg.save_overrides({}, {}, conviction_tiers={"value_over_1m": 4})
        stored = json.loads(overrides.read_text())
        assert stored["conviction_tiers"] == {"value_over_1m": 4}
        assert not any(
            k.startswith("tier_pts_") for k in stored.get("conviction_flags", {})
        ), "the dead tier_pts_ prefix must not come back"

    def test_saving_other_sections_does_not_clobber_tiers(self, overrides):
        cfg.save_overrides({}, {}, conviction_tiers={"value_over_5m": 8})
        cfg.save_overrides({"big_buy_threshold": 42}, {})
        assert cfg.load_config()["alert_rules"]["big_buy_threshold"] == 42
        by_label = {
            label: pts
            for (_, pts, label) in cfg.load_config()["conviction_tiers"]["value"]
        }
        assert by_label["value_over_5m"] == 8


class TestAtomicWrite:
    def test_no_temp_file_is_left_behind(self, tmp_path, monkeypatch):
        path = tmp_path / "config_overrides.json"
        monkeypatch.setattr(cfg, "OVERRIDES_PATH", str(path))
        cfg.save_overrides({"big_buy_threshold": 1}, {})
        assert path.exists()
        assert not (tmp_path / "config_overrides.json.tmp").exists()

    def test_written_file_is_valid_json(self, tmp_path, monkeypatch):
        path = tmp_path / "config_overrides.json"
        monkeypatch.setattr(cfg, "OVERRIDES_PATH", str(path))
        cfg.save_overrides({"big_buy_threshold": 1}, {"min_value": 5})
        json.loads(path.read_text())


class TestConfigVersion:
    def test_stable_for_identical_config(self):
        base = cfg.load_config()
        assert cfg.config_version(base) == cfg.config_version(cfg.load_config())

    def test_changes_when_a_tier_point_changes(self):
        base = cfg.load_config()
        before = cfg.config_version(base)
        base["conviction_tiers"]["value"][0] = (5_000_000, 9, "value_over_5m")
        assert cfg.config_version(base) != before

    def test_changes_when_conviction_flags_change(self):
        base = cfg.load_config()
        before = cfg.config_version(base)
        base["conviction_flags"]["ceo_cfo_bonus"] = 99
        assert cfg.config_version(base) != before

    def test_ignores_credentials(self):
        """A rotated API key must not orphan every cached query."""
        base = cfg.load_config()
        before = cfg.config_version(base)
        base["polygon_api_key"] = "a-different-key"
        assert cfg.config_version(base) == before

    def test_query_cache_key_carries_the_version(self):
        key = app._query_cache_key({"d": "2026-07-24"})
        assert key.startswith("it:query:"), "invalidate_query_cache scans it:query:*"
        assert app._config_version() in key

    def test_config_cache_holds_config_and_version_together(self):
        """maxsize must exceed 1, or the two entries evict each other."""
        assert app._config_cache.maxsize >= 2
