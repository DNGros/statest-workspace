"""
Color mapping for distinctive street name words.

Colors are chosen to reflect the semantic meaning of each word:
- Water features: blues
- Nature/vegetation: greens
- Desert/arid: warm earth tones (browns, oranges, reds)
- Spanish words: warm Mediterranean colors
- French words: elegant purples/lavenders
- Hawaiian words: tropical blues/teals
- Geographic features: earth tones
- Administrative: grays/neutrals
- Religious/historical: muted traditional colors
"""

# Color palette for street name words
# Using hex colors that work well in visualizations
WORD_COLORS = {
    # Water features - blues and teals
    'lake': '#4A90A4',      # Lake blue
    'pond': '#5B9AA8',      # Pond blue (lighter)
    'brook': '#6BA3B8',     # Brook blue (light flowing)
    'creek': '#7BB3C8',     # Creek blue (in stop words, but included for completeness)
    'run': '#4A86A6',       # Stream blue
    'branch': '#3E8FB2',    # Branch blue (tributary)
    'fork': '#89C1D6',      # Fork blue (river split)
    
    # Hawaiian - tropical colors
    'ala': '#20B2AA',       # Light sea green (ala = path)
    'kai': '#00CED1',       # Dark turquoise (kai = sea)
    
    # Spanish/Latino - warm Mediterranean colors
    'calle': '#D4A574',     # Warm tan/adobe
    'camino': '#C89968',    # Dusty road brown
    'via': '#B8895C',       # Pathway brown
    'de': '#CCA57C',        # Spanish connector - neutral tan
    'la': '#D4AD84',        # Spanish article - light tan
    'avenida': '#C09060',   # Avenue brown (in stop words)
    
    # French - elegant purples/lavenders
    'rue': '#9B7EBD',       # French lavender
    'parish': '#8B6FA8',    # Louisiana purple (unique to LA)
    
    # Desert/arid features - warm earth tones
    'canyon': '#CD7F5C',    # Canyon red-brown
    'mesa': '#D4916C',      # Mesa tan
    'peak': '#6E7F91',      # Mountain peak slate
    'valley': '#B8956D',    # Valley golden-brown
    'mountain': '#8B7D6B',  # Mountain gray-brown
    
    # Forest/vegetation - greens
    'forest': '#5F7D5B',    # Forest green
    'pine': '#6B8E67',      # Pine green
    'oak': '#7A9876',       # Oak green (in stop words)
    
    # Geographic/terrain features - earth tones
    'hollow': '#8A7A6E',    # Hollow gray-brown (Appalachian)
    'cove': '#A89B8E',      # Cove sandy-brown
    'point': '#8B8680',     # Point gray
    'bend': '#B09A7D',      # River bend brown
    'trace': '#9D8B7A',     # Historic trace brown
    'ridge': '#8B7E71',     # Ridge brown-gray
    'hill': '#A0937E',      # Hill brown (in stop words)
    
    # Agricultural/rural - muted greens and browns
    'farm': '#8B9B6B',      # Farm green-brown
    'ranch': '#A89560',     # Ranch tan
    'mill': '#8B8070',      # Mill stone gray
    
    # Infrastructure/administrative - grays and neutrals
    'highway': '#7B7B7B',   # Highway gray
    'route': '#8B8B8B',     # Route gray
    'parkway': '#5F7763',   # Parkway green-gray
    'loop': '#747474',      # Loop gray
    'plaza': '#A08E7F',     # Plaza tan-gray
    'terrace': '#A89B8B',   # Terrace beige-gray
    'township': '#7B7B7B',  # Township administrative gray
    'development': '#888888', # Development gray
    'access': '#7A7A7A',    # Access road gray
    
    # Religious/historical - muted traditional colors
    'church': '#8B7B8B',    # Church purple-gray
    'cemetery': '#6B6B6B',  # Cemetery dark gray
    'fort': '#7B6B5B',      # Fort military brown
    'old': '#9B8B7B',       # Old historical brown
    
    # Administrative/location codes - neutral
    'state': '#808080',     # State gray
    'national': '#6B7B6B',  # National park green-gray
    'county': '#888888',    # County gray (in stop words)
    
    # Abbreviations and codes - light grays
    'rd': '#999999',        # Road abbreviation
    'n': '#AAAAAA',         # North abbreviation
    'e': '#AAAAAA',         # East abbreviation
    'sd': '#999999',        # South Dakota code
    'cs': '#999999',        # Code
    'ems': '#999999',       # EMS code
    '1/2': '#BBBBBB',       # Fractional address
    
    # Proper nouns/place names - distinctive colors
    'lauderdale': '#8B7B9B', # Lauderdale purple-gray (Alabama county)
    'smith': '#9B8B8B',     # Smith gray (common name)
    
    # Landscape features - varied earth tones
    'view': '#9BA8B8',      # View blue-gray (vista)
    'fire': '#CD5C5C',      # Fire red
}

def get_word_color(word: str) -> str:
    """
    Get the color for a given word.
    
    Args:
        word: The word to get color for
        
    Returns:
        Hex color string
        
    Raises:
        KeyError: If word is not in the color mapping
    """
    if word not in WORD_COLORS:
        raise KeyError(
            f"Word '{word}' not found in color mapping. "
            f"Please add it to WORD_COLORS in word_colors.py"
        )
    return WORD_COLORS[word]


def get_all_mapped_words() -> list[str]:
    """Get list of all words that have color mappings."""
    return sorted(WORD_COLORS.keys())


if __name__ == "__main__":
    # Print color mapping for verification
    print("Word Color Mapping:")
    print("=" * 60)
    for word in sorted(WORD_COLORS.keys()):
        color = WORD_COLORS[word]
        print(f"{word:20s} → {color}")
    
    print(f"\nTotal words mapped: {len(WORD_COLORS)}")

