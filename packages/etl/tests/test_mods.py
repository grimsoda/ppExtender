"""Tests for mod encoding utilities."""

import pytest


def test_decode_mods_empty():
    """Test decode_mods with 0 returns empty list."""
    from src.mods import decode_mods

    assert decode_mods(0) == []


def test_decode_mods_single_mods():
    """Test decode_mods with single mods."""
    from src.mods import decode_mods, MOD_ACRONYMS

    # Test each single mod
    assert decode_mods(1) == ["NF"]  # No Fail
    assert decode_mods(2) == ["EZ"]  # Easy
    assert decode_mods(4) == ["TD"]  # Touch Device
    assert decode_mods(8) == ["HD"]  # Hidden
    assert decode_mods(16) == ["HR"]  # Hard Rock
    assert decode_mods(32) == ["SD"]  # Sudden Death
    assert decode_mods(64) == ["DT"]  # Double Time
    assert decode_mods(128) == ["RX"]  # Relax
    assert decode_mods(256) == ["HT"]  # Half Time
    assert decode_mods(512) == ["NC"]  # Nightcore
    assert decode_mods(1024) == ["FL"]  # Flashlight


def test_decode_mods_combinations():
    """Test decode_mods with multiple mods."""
    from src.mods import decode_mods

    # DT + HD = 64 + 8 = 72
    result = decode_mods(72)
    assert set(result) == {"DT", "HD"}

    # NF + HR = 1 + 16 = 17
    result = decode_mods(17)
    assert set(result) == {"NF", "HR"}

    # All speed mods: DT + HT = 64 + 256 = 320
    result = decode_mods(320)
    assert set(result) == {"DT", "HT"}


def test_decode_mods_with_nc():
    """Test decode_mods with NC (should include NC)."""
    from src.mods import decode_mods

    # NC = 512
    assert decode_mods(512) == ["NC"]
    # NC + HD = 512 + 8 = 520
    result = decode_mods(520)
    assert set(result) == {"NC", "HD"}


def test_decode_mods_unknown_bits():
    """Test decode_mods with unknown bits - should skip gracefully."""
    from src.mods import decode_mods

    # Unknown bit 2048 + known HD (8) = 2056
    result = decode_mods(2056)
    # Should only return known mods
    assert result == ["HD"]


def test_encode_mods_empty():
    """Test encode_mods with empty list."""
    from src.mods import encode_mods

    assert encode_mods([]) == 0


def test_encode_mods_single_mods():
    """Test encode_mods with single mods."""
    from src.mods import encode_mods

    assert encode_mods(["NF"]) == 1
    assert encode_mods(["EZ"]) == 2
    assert encode_mods(["TD"]) == 4
    assert encode_mods(["HD"]) == 8
    assert encode_mods(["HR"]) == 16
    assert encode_mods(["SD"]) == 32
    assert encode_mods(["DT"]) == 64
    assert encode_mods(["RX"]) == 128
    assert encode_mods(["HT"]) == 256
    assert encode_mods(["NC"]) == 512
    assert encode_mods(["FL"]) == 1024


def test_encode_mods_combinations():
    """Test encode_mods with multiple mods."""
    from src.mods import encode_mods

    # DT + HD = 64 + 8 = 72
    result = encode_mods(["DT", "HD"])
    assert result == 72

    # NF + HR = 1 + 16 = 17
    result = encode_mods(["NF", "HR"])
    assert result == 17


def test_encode_mods_order_independent():
    """Test encode_mods is order independent."""
    from src.mods import encode_mods

    # Order shouldn't matter
    assert encode_mods(["HD", "DT"]) == encode_mods(["DT", "HD"])
    assert encode_mods(["NF", "EZ", "HD"]) == encode_mods(["HD", "NF", "EZ"])


def test_encode_mods_unknown_mods():
    """Test encode_mods with unknown mods - should skip gracefully."""
    from src.mods import encode_mods

    # Unknown mod + known mod
    result = encode_mods(["UNKNOWN", "HD"])
    assert result == 8


def test_normalize_mods_empty():
    """Test normalize_mods with empty list."""
    from src.mods import normalize_mods

    assert normalize_mods([]) == []


def test_normalize_mods_no_change():
    """Test normalize_mods with mods that don't need normalization."""
    from src.mods import normalize_mods

    result = normalize_mods(["DT", "HD"])
    assert set(result) == {"DT", "HD"}


def test_normalize_mods_nc_to_dt():
    """Test normalize_mods converts NC to DT."""
    from src.mods import normalize_mods

    # NC should become DT
    result = normalize_mods(["NC", "HD"])
    assert set(result) == {"DT", "HD"}

    # Just NC should become just DT
    result = normalize_mods(["NC"])
    assert result == ["DT"]


def test_normalize_mods_preserves_other_mods():
    """Test normalize_mods preserves non-NC mods."""
    from src.mods import normalize_mods

    result = normalize_mods(["NC", "HR", "HD", "EZ"])
    assert set(result) == {"DT", "HR", "HD", "EZ"}


def test_get_speed_mod_none():
    """Test get_speed_mod with no speed mods."""
    from src.mods import get_speed_mod

    assert get_speed_mod([]) is None
    assert get_speed_mod(["HD"]) is None
    assert get_speed_mod(["HR", "FL"]) is None


def test_get_speed_mod_dt():
    """Test get_speed_mod with DT."""
    from src.mods import get_speed_mod

    assert get_speed_mod(["DT"]) == "DT"
    assert get_speed_mod(["DT", "HD"]) == "DT"
    assert get_speed_mod(["HR", "DT"]) == "DT"


def test_get_speed_mod_ht():
    """Test get_speed_mod with HT."""
    from src.mods import get_speed_mod

    assert get_speed_mod(["HT"]) == "HT"
    assert get_speed_mod(["HT", "EZ"]) == "HT"


def test_get_speed_mod_nc():
    """Test get_speed_mod with NC (treated as DT)."""
    from src.mods import get_speed_mod

    assert get_speed_mod(["NC"]) == "DT"
    assert get_speed_mod(["NC", "HD"]) == "DT"


def test_get_speed_mod_dt_over_ht():
    """Test get_speed_mod prioritizes DT over HT when both present."""
    from src.mods import get_speed_mod

    # DT + HT - should return DT
    result = get_speed_mod(["DT", "HT"])
    assert result == "DT"


def test_mods_to_key_empty():
    """Test mods_to_key with empty list."""
    from src.mods import mods_to_key

    assert mods_to_key([]) == ""


def test_mods_to_key_single():
    """Test mods_to_key with single mod."""
    from src.mods import mods_to_key

    assert mods_to_key(["HD"]) == "HD"
    assert mods_to_key(["DT"]) == "DT"


def test_mods_to_key_multiple():
    """Test mods_to_key with multiple mods."""
    from src.mods import mods_to_key

    # Should be comma-separated and sorted alphabetically
    result = mods_to_key(["HD", "DT"])
    assert result == "DT,HD"

    result = mods_to_key(["HR", "EZ", "NF"])
    assert result == "EZ,HR,NF"


def test_mods_to_key_unsorted_input():
    """Test mods_to_key sorts unsorted input."""
    from src.mods import mods_to_key

    # Input in any order, output should be sorted
    result = mods_to_key(["HR", "DT", "HD", "EZ"])
    assert result == "DT,EZ,HD,HR"


def test_mods_to_key_with_nc():
    """Test mods_to_key includes NC (not normalized in key)."""
    from src.mods import mods_to_key

    result = mods_to_key(["NC", "HD"])
    # Sorted: HD, NC
    assert result == "HD,NC"


def test_roundtrip():
    """Test encode/decode roundtrip."""
    from src.mods import encode_mods, decode_mods

    # Test various combinations
    test_cases = [
        ["NF"],
        ["DT"],
        ["DT", "HD"],
        ["HR", "DT", "HD"],
        ["EZ", "NF"],
    ]

    for mods in test_cases:
        encoded = encode_mods(mods)
        decoded = decode_mods(encoded)
        # Decoded should match original (order may differ)
        assert set(decoded) == set(mods), (
            f"Failed for {mods}: encoded={encoded}, decoded={decoded}"
        )


def test_mod_acronyms_exists():
    """Test MOD_ACRONYMS dict exists and has expected values."""
    from src.mods import MOD_ACRONYMS

    assert MOD_ACRONYMS[1] == "NF"
    assert MOD_ACRONYMS[2] == "EZ"
    assert MOD_ACRONYMS[4] == "TD"
    assert MOD_ACRONYMS[8] == "HD"
    assert MOD_ACRONYMS[16] == "HR"
    assert MOD_ACRONYMS[32] == "SD"
    assert MOD_ACRONYMS[64] == "DT"
    assert MOD_ACRONYMS[128] == "RX"
    assert MOD_ACRONYMS[256] == "HT"
    assert MOD_ACRONYMS[512] == "NC"
    assert MOD_ACRONYMS[1024] == "FL"
