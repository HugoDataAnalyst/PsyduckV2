import json
import os

# Get the directory where this script is located
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Simple cache to store loaded JSONs
_TRANSLATION_CACHE = {}

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

def translate(key, lang="en"):
    """
    Returns the translated string for the given key and language.
    Falls back to the key itself if the translation is missing.
    """
    translations = get_translation(lang)
    return translations.get(key, key)
