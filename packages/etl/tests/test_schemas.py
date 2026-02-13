"""Schema validation tests for ETL pipeline."""

import pytest
from pydantic import ValidationError

from src.schemas import Score, Beatmap, User, Playcount
from src.validation import (
    validate_row,
    ValidationMode,
    validate_batch,
    get_validation_mode,
)


class TestScoreModel:
    """Test cases for Score model validation."""

    def test_score_valid_data(self):
        """Test Score model with valid data."""
        score = Score(
            score_id=1,
            user_id=123,
            beatmap_id=456,
            pp=350.5,
            mods_key="DT,HR",
            speed_mod="DT",
            accuracy=98.5,
            score=1000000,
            data="{}",
        )
        assert score.score_id == 1
        assert score.user_id == 123
        assert score.beatmap_id == 456
        assert score.pp == 350.5
        assert score.accuracy == 98.5

    def test_score_invalid_pp_negative(self):
        """Test Score model with negative pp (should fail)."""
        with pytest.raises(ValidationError) as exc_info:
            Score(
                score_id=1,
                user_id=123,
                beatmap_id=456,
                pp=-10.0,  # Invalid: negative pp
                mods_key="DT",
                speed_mod="DT",
                accuracy=98.5,
                score=1000000,
                data="{}",
            )
        assert "pp" in str(exc_info.value)

    def test_score_invalid_accuracy_over_100(self):
        """Test Score model with accuracy > 100 (should fail)."""
        with pytest.raises(ValidationError) as exc_info:
            Score(
                score_id=1,
                user_id=123,
                beatmap_id=456,
                pp=350.5,
                mods_key="DT",
                speed_mod="DT",
                accuracy=105.0,  # Invalid: > 100
                score=1000000,
                data="{}",
            )
        assert "accuracy" in str(exc_info.value)

    def test_score_invalid_accuracy_negative(self):
        """Test Score model with negative accuracy (should fail)."""
        with pytest.raises(ValidationError) as exc_info:
            Score(
                score_id=1,
                user_id=123,
                beatmap_id=456,
                pp=350.5,
                mods_key="DT",
                speed_mod="DT",
                accuracy=-5.0,  # Invalid: negative
                score=1000000,
                data="{}",
            )
        assert "accuracy" in str(exc_info.value)

    def test_score_type_coercion_string_to_int(self):
        """Test Score model type coercion from string to int."""
        score = Score(
            score_id="1",  # String input
            user_id="123",  # String input
            beatmap_id="456",  # String input
            pp=350.5,
            mods_key="DT",
            speed_mod="DT",
            accuracy=98.5,
            score="1000000",  # String input
            data="{}",
        )
        assert score.score_id == 1
        assert score.user_id == 123
        assert isinstance(score.score_id, int)
        assert isinstance(score.user_id, int)

    def test_score_required_fields(self):
        """Test Score model required fields enforcement."""
        with pytest.raises(ValidationError) as exc_info:
            Score(
                user_id=123,
                beatmap_id=456,
                pp=350.5,
                mods_key="DT",
                # Missing required fields
            )
        assert "score_id" in str(exc_info.value)


class TestBeatmapModel:
    """Test cases for Beatmap model validation."""

    def test_beatmap_valid_data(self):
        """Test Beatmap model with valid data."""
        beatmap = Beatmap(
            beatmap_id=12345,
            beatmapset_id=67890,
            version="Hard",
            difficultyrating=4.5,
            bpm=180.0,
            total_length=120,
            playmode=0,
        )
        assert beatmap.beatmap_id == 12345
        assert beatmap.difficultyrating == 4.5
        assert beatmap.playmode == 0

    def test_beatmap_type_coercion(self):
        """Test Beatmap model type coercion."""
        beatmap = Beatmap(
            beatmap_id="12345",  # String
            beatmapset_id="67890",  # String
            version="Hard",
            difficultyrating="4.5",  # String
            bpm="180.0",  # String
            total_length="120",  # String
            playmode="0",  # String
        )
        assert beatmap.beatmap_id == 12345
        assert beatmap.difficultyrating == 4.5
        assert isinstance(beatmap.beatmap_id, int)

    def test_beatmap_required_fields(self):
        """Test Beatmap model required fields."""
        with pytest.raises(ValidationError):
            Beatmap(
                version="Hard"
                # Missing required beatmap_id
            )


class TestUserModel:
    """Test cases for User model validation."""

    def test_user_valid_data(self):
        """Test User model with valid data."""
        user = User(
            user_id=12345,
            username="testuser",
            pp_raw=2850.5,
            accuracy=98.5,
            playcount=5000,
        )
        assert user.user_id == 12345
        assert user.username == "testuser"
        assert user.pp_raw == 2850.5

    def test_user_invalid_pp_negative(self):
        """Test User model with negative pp_raw."""
        with pytest.raises(ValidationError) as exc_info:
            User(
                user_id=12345,
                username="testuser",
                pp_raw=-100.0,  # Invalid
                accuracy=98.5,
                playcount=5000,
            )
        assert "pp_raw" in str(exc_info.value)

    def test_user_invalid_accuracy_range(self):
        """Test User model with invalid accuracy range."""
        with pytest.raises(ValidationError) as exc_info:
            User(
                user_id=12345,
                username="testuser",
                pp_raw=2850.5,
                accuracy=150.0,  # Invalid: > 100
                playcount=5000,
            )
        assert "accuracy" in str(exc_info.value)

    def test_user_type_coercion(self):
        """Test User model type coercion."""
        user = User(
            user_id="12345",  # String
            username=12345,  # Int, should convert to string
            pp_raw="2850.5",  # String
            accuracy="98.5",  # String
            playcount="5000",  # String
        )
        assert user.user_id == 12345
        assert user.username == "12345"
        assert isinstance(user.playcount, int)


class TestPlaycountModel:
    """Test cases for Playcount model validation."""

    def test_playcount_valid_data(self):
        """Test Playcount model with valid data."""
        playcount = Playcount(user_id=12345, beatmap_id=67890, playcount=50)
        assert playcount.user_id == 12345
        assert playcount.beatmap_id == 67890
        assert playcount.playcount == 50

    def test_playcount_invalid_negative(self):
        """Test Playcount model with negative playcount."""
        with pytest.raises(ValidationError) as exc_info:
            Playcount(
                user_id=12345,
                beatmap_id=67890,
                playcount=-5,  # Invalid
            )
        assert "playcount" in str(exc_info.value)

    def test_playcount_type_coercion(self):
        """Test Playcount model type coercion."""
        playcount = Playcount(
            user_id="12345",  # String
            beatmap_id="67890",  # String
            playcount="50",  # String
        )
        assert playcount.user_id == 12345
        assert playcount.playcount == 50


class TestValidateRow:
    """Test cases for validate_row function."""

    def test_validate_row_score_valid(self):
        """Test validate_row with valid Score data."""
        row = {
            "score_id": 1,
            "user_id": 123,
            "beatmap_id": 456,
            "pp": 350.5,
            "mods_key": "DT",
            "speed_mod": "DT",
            "accuracy": 98.5,
            "score": 1000000,
            "data": "{}",
        }
        result = validate_row("scores", row)
        assert result.score_id == 1
        assert isinstance(result, Score)

    def test_validate_row_beatmap_valid(self):
        """Test validate_row with valid Beatmap data."""
        row = {
            "beatmap_id": 12345,
            "beatmapset_id": 67890,
            "version": "Hard",
            "difficultyrating": 4.5,
            "bpm": 180.0,
            "total_length": 120,
            "playmode": 0,
        }
        result = validate_row("beatmaps", row)
        assert result.beatmap_id == 12345
        assert isinstance(result, Beatmap)

    def test_validate_row_user_valid(self):
        """Test validate_row with valid User data."""
        row = {
            "user_id": 12345,
            "username": "testuser",
            "pp_raw": 2850.5,
            "accuracy": 98.5,
            "playcount": 5000,
        }
        result = validate_row("users", row)
        assert result.user_id == 12345
        assert isinstance(result, User)

    def test_validate_row_playcount_valid(self):
        """Test validate_row with valid Playcount data."""
        row = {"user_id": 12345, "beatmap_id": 67890, "playcount": 50}
        result = validate_row("playcount", row)
        assert result.user_id == 12345
        assert isinstance(result, Playcount)

    def test_validate_row_invalid_table(self):
        """Test validate_row with invalid table name."""
        with pytest.raises(ValueError) as exc_info:
            validate_row("invalid_table", {})
        assert "invalid_table" in str(exc_info.value)

    def test_validate_row_invalid_data_strict(self):
        """Test validate_row with invalid data in strict mode."""
        row = {
            "score_id": 1,
            "user_id": 123,
            "beatmap_id": 456,
            "pp": -10.0,  # Invalid
            "mods_key": "DT",
            "speed_mod": "DT",
            "accuracy": 98.5,
            "score": 1000000,
            "data": "{}",
        }
        with pytest.raises(ValidationError):
            validate_row("scores", row, mode=ValidationMode.STRICT)

    def test_validate_row_invalid_data_lenient(self):
        """Test validate_row with invalid data in lenient mode returns None."""
        row = {
            "score_id": 1,
            "user_id": 123,
            "beatmap_id": 456,
            "pp": -10.0,  # Invalid
            "mods_key": "DT",
            "speed_mod": "DT",
            "accuracy": 98.5,
            "score": 1000000,
            "data": "{}",
        }
        result = validate_row("scores", row, mode=ValidationMode.LENIENT)
        assert result is None

    def test_validate_row_none_mode(self):
        """Test validate_row with mode=NONE returns None without validation."""
        row = {"invalid": "data"}
        result = validate_row("scores", row, mode=ValidationMode.NONE)
        assert result is None

    def test_validate_batch(self):
        """Test validate_batch with multiple rows."""
        rows = [
            {"user_id": 12345, "beatmap_id": 67890, "playcount": 50},
            {"user_id": 12346, "beatmap_id": 67891, "playcount": 30},
        ]
        results = validate_batch("playcount", rows)
        assert len(results) == 2
        assert all(isinstance(r, Playcount) for r in results)

    def test_validate_batch_with_invalid(self):
        """Test validate_batch filters invalid rows in lenient mode."""
        rows = [
            {"user_id": 12345, "beatmap_id": 67890, "playcount": 50},
            {"user_id": 12346, "beatmap_id": 67891, "playcount": -5},  # Invalid
        ]
        results = validate_batch("playcount", rows, mode=ValidationMode.LENIENT)
        assert len(results) == 1

    def test_get_validation_mode_from_env_default(self):
        """Test get_validation_mode returns default when env var not set."""
        import os

        original = os.environ.pop("PYDANTIC_VALIDATION", None)
        try:
            mode = get_validation_mode()
            assert mode == ValidationMode.STRICT
        finally:
            if original:
                os.environ["PYDANTIC_VALIDATION"] = original

    def test_get_validation_mode_from_env_invalid(self):
        """Test get_validation_mode handles invalid env var gracefully."""
        import os

        original = os.environ.get("PYDANTIC_VALIDATION")
        os.environ["PYDANTIC_VALIDATION"] = "invalid_mode"
        try:
            mode = get_validation_mode()
            assert mode == ValidationMode.STRICT  # Falls back to strict
        finally:
            if original:
                os.environ["PYDANTIC_VALIDATION"] = original
            else:
                os.environ.pop("PYDANTIC_VALIDATION", None)

    def test_validate_row_table_aliases(self):
        """Test validate_row works with table name aliases."""
        row = {"user_id": 12345, "beatmap_id": 67890, "playcount": 50}

        # Test various aliases
        for table_name in ["playcount", "osu_user_beatmap_playcount"]:
            result = validate_row(table_name, row)
            assert isinstance(result, Playcount)
