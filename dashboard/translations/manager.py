import json
import os

# Get the directory where this script is located
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Simple cache to store loaded JSONs
_TRANSLATION_CACHE = {}
_POKEMON_TRANSLATION_CACHE = {}

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
