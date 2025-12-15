import json
import os

# Get the directory where this script is located
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Simple cache to store loaded JSONs
_TRANSLATION_CACHE = {}
_POKEMON_TRANSLATION_CACHE = {}
_INVASIONS_TRANSLATION_CACHE = {}
_QUESTS_TRANSLATION_CACHE = {}

def get_translation(lang_code):
    """Loads the JSON file for the given language code."""
    if lang_code in _TRANSLATION_CACHE:
        return _TRANSLATION_CACHE[lang_code]

    try:
        file_path = os.path.join(BASE_DIR, f"{lang_code}.json")
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            _TRANSLATION_CACHE[lang_code] = data
            return data
    except Exception as e:
        print(f"Error loading translation for {lang_code}: {e}")
        return {}

def get_pokemon_translation(lang_code):
    """Loads the Pokemon translation JSON file for the given language code."""
    if lang_code in _POKEMON_TRANSLATION_CACHE:
        return _POKEMON_TRANSLATION_CACHE[lang_code]

    try:
        file_path = os.path.join(BASE_DIR, "pokemon", f"{lang_code}.json")
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            _POKEMON_TRANSLATION_CACHE[lang_code] = data
            return data
    except Exception as e:
        print(f"Error loading Pokemon translation for {lang_code}: {e}")
        # Fall back to English if the requested language is not available
        if lang_code != "en":
            return get_pokemon_translation("en")
        return {}

def get_invasions_translation(lang_code):
    """Loads the Invasions translation JSON file for the given language code."""
    if lang_code in _INVASIONS_TRANSLATION_CACHE:
        return _INVASIONS_TRANSLATION_CACHE[lang_code]

    try:
        file_path = os.path.join(BASE_DIR, "invasions", f"{lang_code}.json")
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            _INVASIONS_TRANSLATION_CACHE[lang_code] = data
            return data
    except Exception as e:
        print(f"Error loading Invasions translation for {lang_code}: {e}")
        # Fall back to English if the requested language is not available
        if lang_code != "en":
            return get_invasions_translation("en")
        return {}

def get_quests_translation(lang_code):
    """Loads the Quests translation JSON file for the given language code."""
    if lang_code in _QUESTS_TRANSLATION_CACHE:
        return _QUESTS_TRANSLATION_CACHE[lang_code]

    try:
        file_path = os.path.join(BASE_DIR, "quests", f"{lang_code}.json")
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            _QUESTS_TRANSLATION_CACHE[lang_code] = data
            return data
    except Exception as e:
        print(f"Error loading Quests translation for {lang_code}: {e}")
        # Fall back to English if the requested language is not available
        if lang_code != "en":
            return get_quests_translation("en")
        return {}

def translate(key, lang="en"):
    """
    Returns the translated string for the given key and language.
    Falls back to the key itself if the translation is missing.
    """
    translations = get_translation(lang)
    return translations.get(key, key)

def translate_pokemon(pokemon_id, lang="en"):
    """
    Returns the translated Pokemon name for the given Pokemon ID and language.
    Falls back to English if the translation is missing, then to 'Pokemon #ID'.

    Args:
        pokemon_id: The Pokemon's national dex number (int or str)
        lang: The language code (default: "en")

    Returns:
        The translated Pokemon name
    """
    translations = get_pokemon_translation(lang)
    pid_str = str(pokemon_id)

    # Try to get from current language
    if pid_str in translations:
        return translations[pid_str]

    # Fallback to English
    if lang != "en":
        en_translations = get_pokemon_translation("en")
        if pid_str in en_translations:
            return en_translations[pid_str]

    # Final fallback
    return f"Pokemon #{pokemon_id}"


def translate_invader(invader_id, lang="en"):
    """
    Returns the translated invader/grunt name for the given ID and language.
    Falls back to English if the translation is missing, then to 'Grunt #ID'.

    Args:
        invader_id: The invader/grunt character ID (int or str)
        lang: The language code (default: "en")

    Returns:
        The translated invader name
    """
    translations = get_invasions_translation(lang)
    invaders = translations.get("invaders", {})
    id_str = str(invader_id)

    # Try to get from current language
    if id_str in invaders:
        return invaders[id_str]

    # Fallback to English
    if lang != "en":
        en_translations = get_invasions_translation("en")
        en_invaders = en_translations.get("invaders", {})
        if id_str in en_invaders:
            return en_invaders[id_str]

    # Final fallback
    return f"Grunt #{invader_id}"


def translate_incident_display(display_id, lang="en"):
    """
    Returns the translated incident display type name for the given ID and language.
    Falls back to English if the translation is missing, then to 'Type #ID'.

    Args:
        display_id: The incident display type ID (int or str)
        lang: The language code (default: "en")

    Returns:
        The translated incident display type name
    """
    translations = get_invasions_translation(lang)
    displays = translations.get("incident_displays", {})
    id_str = str(display_id)

    # Try to get from current language
    if id_str in displays:
        return displays[id_str]

    # Fallback to English
    if lang != "en":
        en_translations = get_invasions_translation("en")
        en_displays = en_translations.get("incident_displays", {})
        if id_str in en_displays:
            return en_displays[id_str]

    # Final fallback
    return f"Type #{display_id}"


def translate_quest_type(quest_type_id, lang="en"):
    """
    Returns the translated quest type name for the given ID and language.
    Falls back to English if the translation is missing, then to 'Quest Type #ID'.

    Args:
        quest_type_id: The quest type ID (int or str)
        lang: The language code (default: "en")

    Returns:
        The translated quest type name
    """
    translations = get_quests_translation(lang)
    types = translations.get("types", {})
    id_str = str(quest_type_id)

    # Try to get from current language
    if id_str in types:
        return types[id_str]

    # Fallback to English
    if lang != "en":
        en_translations = get_quests_translation("en")
        en_types = en_translations.get("types", {})
        if id_str in en_types:
            return en_types[id_str]

    # Final fallback
    return f"Quest Type #{quest_type_id}"


def translate_quest_reward(reward_type_id, lang="en"):
    """
    Returns the translated quest reward type name for the given ID and language.
    Falls back to English if the translation is missing, then to 'Reward Type #ID'.

    Args:
        reward_type_id: The reward type ID (int or str)
        lang: The language code (default: "en")

    Returns:
        The translated reward type name
    """
    translations = get_quests_translation(lang)
    rewards = translations.get("rewards", {})
    id_str = str(reward_type_id)

    # Try to get from current language
    if id_str in rewards:
        return rewards[id_str]

    # Fallback to English
    if lang != "en":
        en_translations = get_quests_translation("en")
        en_rewards = en_translations.get("rewards", {})
        if id_str in en_rewards:
            return en_rewards[id_str]

    # Final fallback
    return f"Reward Type #{reward_type_id}"


def translate_quest_item(item_id, lang="en"):
    """
    Returns the translated quest item name for the given ID and language.
    Falls back to English if the translation is missing, then to 'Item #ID'.

    Args:
        item_id: The item ID (int or str)
        lang: The language code (default: "en")

    Returns:
        The translated item name
    """
    translations = get_quests_translation(lang)
    items = translations.get("items", {})
    id_str = str(item_id)

    # Try to get from current language
    if id_str in items:
        return items[id_str]

    # Fallback to English
    if lang != "en":
        en_translations = get_quests_translation("en")
        en_items = en_translations.get("items", {})
        if id_str in en_items:
            return en_items[id_str]

    # Final fallback
    return f"Item #{item_id}"
