"""List of US President surnames for street name analysis."""

# All US Presidents' surnames (as of 2025)
# Includes all 46 presidencies (45 individuals - Cleveland served twice)
PRESIDENT_SURNAMES = [
    "Washington",
    "Adams",
    "Jefferson",
    "Madison",
    "Monroe",
    "Jackson",
    "Van Buren",
    "Harrison",
    "Tyler",
    "Polk",
    "Taylor",
    "Fillmore",
    "Pierce",
    "Buchanan",
    "Lincoln",
    "Johnson",
    "Grant",
    "Hayes",
    "Garfield",
    "Arthur",
    "Cleveland",
    "McKinley",
    "Roosevelt",
    "Taft",
    "Wilson",
    "Harding",
    "Coolidge",
    "Hoover",
    "Truman",
    "Eisenhower",
    "Kennedy",
    "Nixon",
    "Ford",
    "Carter",
    "Reagan",
    "Bush",
    "Clinton",
    "Obama",
    "Trump",
    "Biden",
]

# For multi-word surnames, we'll want to check for the full name
# Van Buren is the only multi-word surname
MULTIWORD_SURNAMES = ["Van Buren"]

def get_president_surnames():
    """Return list of all US president surnames."""
    return PRESIDENT_SURNAMES.copy()

def get_multiword_surnames():
    """Return list of multi-word president surnames."""
    return MULTIWORD_SURNAMES.copy()


