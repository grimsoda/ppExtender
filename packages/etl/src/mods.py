"""Mod encoding utilities for osu! mods.

This module provides functions to encode/decode osu! mod combinations
using bitset representation. Mods are represented as bit flags where
each mod has a unique bit value.

Mod Bit Values (osu! API):
    1: NF (No Fail)
    2: EZ (Easy)
    4: TD (Touch Device)
    8: HD (Hidden)
    16: HR (Hard Rock)
    32: SD (Sudden Death)
    64: DT (Double Time)
    128: RX (Relax)
    256: HT (Half Time)
    512: NC (Nightcore)
    1024: FL (Flashlight)

Note: NC (512) is typically treated as DT for consistency, but both
representations are supported.
"""

from typing import List, Optional


#: Mapping of bit value to mod acronym
MOD_ACRONYMS = {
    1: "NF",  # No Fail
    2: "EZ",  # Easy
    4: "TD",  # Touch Device
    8: "HD",  # Hidden
    16: "HR",  # Hard Rock
    32: "SD",  # Sudden Death
    64: "DT",  # Double Time
    128: "RX",  # Relax
    256: "HT",  # Half Time
    512: "NC",  # Nightcore
    1024: "FL",  # Flashlight
}

#: Reverse mapping: acronym to bit value
ACRONYM_BITS = {v: k for k, v in MOD_ACRONYMS.items()}

#: Speed mods for categorization
SPEED_MODS = {"DT", "HT", "NC"}


def decode_mods(bitset: int) -> List[str]:
    """Decode a bitset into a list of mod acronyms.

    Args:
        bitset: Integer representing the mod combination as bit flags.

    Returns:
        List of mod acronyms (e.g., ['DT', 'HD']).
        Unknown bits are silently skipped.

    Example:
        >>> decode_mods(72)  # DT (64) + HD (8)
        ['DT', 'HD']
        >>> decode_mods(0)
        []
    """
    if bitset == 0:
        return []

    mods = []
    for bit, acronym in MOD_ACRONYMS.items():
        if bitset & bit:
            mods.append(acronym)
    return mods


def encode_mods(acronyms: List[str]) -> int:
    """Encode a list of mod acronyms into a bitset.

    Args:
        acronyms: List of mod acronyms (e.g., ['DT', 'HD']).

    Returns:
        Integer bitset representing the mod combination.
        Unknown acronyms are silently skipped.

    Example:
        >>> encode_mods(['DT', 'HD'])  # 64 + 8
        72
        >>> encode_mods([])
        0
    """
    bitset = 0
    for acronym in acronyms:
        if acronym in ACRONYM_BITS:
            bitset |= ACRONYM_BITS[acronym]
    return bitset


def normalize_mods(acronyms: List[str]) -> List[str]:
    """Normalize a list of mod acronyms.

    Normalization rules:
    - NC (Nightcore) is converted to DT (Double Time)
    - Unknown mods are preserved as-is
    - Duplicates are removed

    Args:
        acronyms: List of mod acronyms.

    Returns:
        Normalized list of mod acronyms.

    Example:
        >>> normalize_mods(['NC', 'HD'])
        ['DT', 'HD']
        >>> normalize_mods(['DT', 'HD'])
        ['DT', 'HD']
    """
    normalized = []
    seen = set()

    for acronym in acronyms:
        # Normalize NC to DT
        if acronym == "NC":
            acronym = "DT"

        if acronym not in seen:
            normalized.append(acronym)
            seen.add(acronym)

    return normalized


def get_speed_mod(acronyms: List[str]) -> Optional[str]:
    """Get the speed mod from a list of mods.

    Speed mods are: DT (Double Time), HT (Half Time), NC (Nightcore).
    NC is treated as DT. If both DT and HT are present, DT takes priority.

    Args:
        acronyms: List of mod acronyms.

    Returns:
        'DT' if DT or NC is present, 'HT' if HT is present (and no DT),
        or None if no speed mod is present.

    Example:
        >>> get_speed_mod(['DT', 'HD'])
        'DT'
        >>> get_speed_mod(['NC'])  # Nightcore treated as DT
        'DT'
        >>> get_speed_mod(['HT', 'EZ'])
        'HT'
        >>> get_speed_mod(['HD', 'HR'])
        None
    """
    has_dt = "DT" in acronyms or "NC" in acronyms
    has_ht = "HT" in acronyms

    if has_dt:
        return "DT"
    elif has_ht:
        return "HT"
    else:
        return None


def mods_to_key(acronyms: List[str]) -> str:
    """Convert a list of mod acronyms to a canonical key string.

    The key is comma-separated and alphabetically sorted for consistency.
    This ensures that ['HD', 'DT'] and ['DT', 'HD'] produce the same key.

    Args:
        acronyms: List of mod acronyms.

    Returns:
        Comma-separated, sorted string of mods. Empty string if no mods.

    Example:
        >>> mods_to_key(['HD', 'DT'])
        'DT,HD'
        >>> mods_to_key(['NF', 'HR', 'HD'])
        'HD,HR,NF'
        >>> mods_to_key([])
        ''
    """
    if not acronyms:
        return ""

    sorted_mods = sorted(acronyms)
    return ",".join(sorted_mods)
