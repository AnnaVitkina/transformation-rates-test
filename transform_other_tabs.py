"""
Data transformation for the non-MainCosts Excel tabs.

This module handles the data preparation for:
  - AddedRates tab   (pivot_added_rates)
  - CountryZoning tab (flatten_array_data with country-code enrichment)
  - AdditionalZoning, ZoningMatrix, AdditionalCostsPart1, AdditionalCostsPart2
    (flatten_array_data – generic flat pass-through)
  - GoGreenPlusCost (flatten_array_data – countries → codes; other text unchanged)

Public functions:
  flatten_array_data   – converts a JSON array into flat rows, with special handling
                         for CountryZoning (forward-fill RateName + add Country Code)
                         and GoGreenPlusCost (countries → ISO codes; non-country segments unchanged)
  pivot_added_rates    – untangles the interleaved header/data rows in AddedRates

Private helpers:
  _transform_rate_name_to_short
  _fill_country_zoning_rate_names
  _load_country_codes
  _country_to_code
  _fill_country_zoning_country_codes
  _gogreen_country_list_to_codes
  _apply_gogreen_plus_cost_country_codes
  _is_added_rates_header_row
"""

import re
from pathlib import Path


# ---------------------------------------------------------------------------
# CountryZoning helpers
# ---------------------------------------------------------------------------

def _transform_rate_name_to_short(rate_name):
    """
    Convert a long rate card name into a short, underscore-separated code.

    WHY THIS IS NEEDED:
    In the CountryZoning tab, only the first row of each zone block has a full
    RateName (e.g. "DHL EXPRESS WORLDWIDE EXPORT ZONING").  The rows that follow
    (one per country) have an empty RateName.  We fill those empty cells with a
    short version of the name plus the zone, e.g. "WW_EXP_ZONE_Zone 1".

    TRANSFORMATION RULES (applied in order):
      Rate names containing "Transit Times" (e.g. "DHL EXPRESS Belgium TD International
        Export & TD International Import - Transit Times") -> "WW_EXP_IMP_TRANSIT_TIMES"
        so they do not mix with normal WW_EXP_IMP zones (WORLDWIDE EXPORT/IMPORT).
      "DHL EXPRESS"    -> removed entirely (it's on every name, adds no value)
      "THIRD COUNTRY"  -> "3RD_COUNTRY"
      "WORLDWIDE"      -> "WW"  (same meaning as INTERNATIONAL)
      "INTERNATIONAL"  -> "WW"  (worldwide)
      "MEDICAL"        -> "MED"
      "BREAKBULK"      -> "BBX"
      "IMPORT"         -> "IMP"
      "EXPORT"         -> "EXP"
      "ZONING"         -> "ZONE"

    The surviving tokens are then assembled in a fixed order so the result is
    always consistent regardless of the original word order:
      e.g. "DHL EXPRESS WORLDWIDE EXPORT ZONING"      -> "WW_EXP_ZONE"
      e.g. "DHL EXPRESS INTERNATIONAL EXPORT ZONING" -> "WW_EXP_ZONE"
      e.g. "DHL EXPRESS ... Transit Times"            -> "WW_EXP_IMP_TRANSIT_TIMES"
    """
    if not rate_name or not isinstance(rate_name, str):
        return ''
    s = rate_name.upper().strip()

    # Transit Times services get a distinct prefix so they don't share WW_EXP_IMP_1, etc. with WORLDWIDE EXPORT/IMPORT
    if 'TRANSIT TIMES' in s:
        return 'WW_EXP_IMP_TRANSIT_TIMES'

    s = s.replace('DHL EXPRESS', ' ')
    s = s.replace('THIRD COUNTRY', ' 3RD_COUNTRY ')
    s = s.replace('WORLDWIDE', ' WW ')       # treat WORLDWIDE same as INTERNATIONAL
    s = s.replace('INTERNATIONAL', ' WW ')
    s = s.replace('MEDICAL', ' MED ')
    s = s.replace('BREAKBULK', ' BBX ')
    s = s.replace('IMPORT', ' IMP ')
    s = s.replace('EXPORT', ' EXP ')
    s = s.replace('ZONING', ' ZONE ')

    tokens = []
    for token in ('WW', '3RD_COUNTRY', 'DOMESTIC', 'ECONOMY', 'MED', 'BBX', 'EXP', 'IMP', 'ZONE'):
        if token in s and token not in tokens:
            tokens.append(token)

    return '_'.join(tokens) if tokens else ''


def _fill_country_zoning_rate_names(rows):
    """
    Fill in the empty RateName cells in the CountryZoning rows.

    THE PROBLEM:
    In the source JSON, the CountryZoning data looks like this:
        Row 1: RateName="DHL EXPRESS WW EXPORT ZONING", Zone="Zone 1", Country="France"
        Row 2: RateName="",                             Zone="Zone 1", Country="Germany"
        Row 3: RateName="",                             Zone="Zone 1", Country="Spain"
        Row 4: RateName="DHL EXPRESS WW EXPORT ZONING", Zone="Zone 2", Country="France"
        ...

    Only the first row of each block has a RateName.  The rest are empty.
    We need to fill them in so every row has a meaningful RateName.

    THE SOLUTION:
    Walk through all rows in order, remembering the last non-empty RateName seen.
    When we find a row with an empty RateName, build a short name from the last
    remembered name plus the current Zone value.

    Example result for rows 2 and 3 above:
        RateName = "WW_EXP_ZONE_Zone 1"
    """
    last_rate_name = ''

    for row in rows:
        rate_name = row.get('RateName') or ''
        zone = row.get('Zone') or ''

        if rate_name:
            last_rate_name = rate_name

        if not rate_name and last_rate_name and zone:
            prefix = _transform_rate_name_to_short(last_rate_name)
            if prefix:
                row['RateName'] = f"{prefix}_{zone}"


# ---------------------------------------------------------------------------
# Country code lookup
# ---------------------------------------------------------------------------

def _load_country_codes(codes_path=None):
    """
    Load the country-name-to-ISO-code dictionary from a plain text file.

    FILE FORMAT (one country per line, tab-separated):
        France    FR
        Germany   DE
        China     CN,CHN

    If a country has multiple codes separated by commas, only the first is used.

    The file is looked for in two locations (in order):
      1. input/dhl_country_codes.txt   (next to this script)
      2. addition/dhl_country_codes.txt

    Returns a dictionary like: {"France": "FR", "Germany": "DE", "China": "CN"}
    Returns an empty dict {} if the file is not found.
    """
    if codes_path is None:
        base = Path(__file__).resolve().parent
        codes_path = base / "input" / "dhl_country_codes.txt"
        if not codes_path.exists():
            codes_path = base / "addition" / "dhl_country_codes.txt"
    print(f"[*] CountryCode Debug: trying codes file: {codes_path}")
    codes_path = Path(codes_path)
    if not codes_path.exists():
        print(f"[WARN] CountryCode Debug: codes file not found: {codes_path}")
        return {}

    name_to_code = {}

    for line in codes_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or "\t" not in line:
            continue

        name, code = line.split("\t", 1)
        name = name.strip()
        code = code.strip()

        if "," in code:
            code = code.split(",")[0].strip()

        if name:
            name_to_code[name] = code

    print(f"[OK] CountryCode Debug: loaded mappings: {len(name_to_code)}")
    if name_to_code:
        sample_items = list(name_to_code.items())[:5]
        print(f"[*] CountryCode Debug: sample mappings: {sample_items}")
    return name_to_code


def _country_to_code(country, name_to_code):
    """
    Look up the ISO country code for a given country name string.

    The country names in the rate card data are not always written exactly the same
    way as in the reference file.  This function tries several variations to find a match.

    LOOKUP ATTEMPTS (in order, returns the first match found):
      1. Exact match as-is                   e.g. "France" -> "FR"
      2. Uppercase version                   e.g. "france" -> "FRANCE" -> "FR"
      3. Common name normalizations:
           "Republic Of" <-> "Rep. Of"       e.g. "Republic Of Korea" -> "Rep. Of Korea"
           " And " <-> " & "                 e.g. "Bosnia And Herzegovina" -> "Bosnia & Herzegovina"
           Strip ", Peoples Republic" etc.   e.g. "China, Peoples Republic" -> "China"
      4. Embedded code fallback:
           If the input is "Afghanistan (AF)", extract "AF" as a last resort.

    Returns the 2-letter (or 3-letter) code string, or '' if nothing matched.
    """
    if not country:
        return ''
    s = str(country).strip()
    if not s:
        return ''

    # Check if the country string already contains an ISO code in parentheses,
    # e.g. "Afghanistan (AF)".  Save the code as a fallback in case name lookup fails.
    paren_code = ''
    m = re.match(r'^(.*?)\s*\(([A-Za-z]{2,3})\)\s*$', s)
    if m:
        s = m.group(1).strip()
        paren_code = m.group(2).upper()

    # Attempt 1: exact match
    code = name_to_code.get(s)
    if code is not None:
        return code

    # Attempt 2: uppercase exact match
    code = name_to_code.get(s.upper())
    if code is not None:
        return code

    # Attempt 2b: case-insensitive match (file may have "Kosovo", data may have "KOSOVO")
    s_upper = s.upper()
    for key, val in name_to_code.items():
        if key.upper() == s_upper:
            return val

    # Attempt 3: normalised variants
    variants = []
    n = s.replace("Republic Of", "Rep. Of").replace("Republic of", "Rep. Of")
    n = n.replace(", Republic", ", Rep.").replace(" Republic", " Rep.")
    variants.append(n)
    variants.append(n.replace(" And ", " & "))
    variants.append(n.replace(" & ", " And "))

    # Handle "Name, The" <-> "The Name" pattern
    # e.g. "Netherlands" -> try "Netherlands, The"
    #      "Netherlands, The" -> try "Netherlands"
    if n.endswith(", The"):
        variants.append(n[:-5].strip())           # "Netherlands, The" -> "Netherlands"
    else:
        variants.append(f"{n}, The")              # "Netherlands" -> "Netherlands, The"
    if n.lower().startswith("the "):
        variants.append(n[4:].strip() + ", The")  # "The Netherlands" -> "Netherlands, The"

    for suffix in (", Peoples Republic", ", People's Republic", ", Peoples Rep.", ", People's Rep.",
                   " Peoples Republic", " People's Republic"):
        if n.endswith(suffix) or suffix in n:
            base = n.replace(suffix, "").strip().strip(",").strip()
            if base:
                variants.append(base)

    for v in variants:
        if not v:
            continue
        code = name_to_code.get(v)
        if code is not None:
            return code
        code = name_to_code.get(v.upper())
        if code is not None:
            return code

    # Attempt 4: use the embedded parenthetical code as a last resort
    if paren_code:
        return paren_code

    return ''


# ---------------------------------------------------------------------------
# GoGreenPlusCost: Origin / Destination country lists → DHL codes
# ---------------------------------------------------------------------------

def _gogreen_segment_to_code(segment, name_to_code):
    """
    Turn one comma-separated segment into a single ISO-style code using dhl_country_codes.txt.

    Expected segment shapes:
      - "ES - Spain"   (code - name): prefer lookup by country name (right side), else left if 2 letters
      - "Spain"        (name only): _country_to_code
      - "ES"           (code only): if 2 letters, return as-is
    """
    s = (segment or '').strip()
    if not s:
        return ''

    if ' - ' in s:
        left, right = s.split(' - ', 1)
        left, right = left.strip(), right.strip()
        code = _country_to_code(right, name_to_code)
        if code:
            return code
        if len(left) == 2 and left.isalpha():
            return left.upper()
        code = _country_to_code(left, name_to_code)
        if code:
            return code
        return ''

    if len(s) == 2 and s.isalpha():
        return s.upper()

    return _country_to_code(s, name_to_code) or ''


def _gogreen_country_list_to_codes(text, name_to_code):
    """
    Convert a comma-separated list like "ES - Spain, IT - Italy" into "ES, IT"
    using lookups from dhl_country_codes.txt (via _country_to_code).

    Segments that do not resolve to a code (e.g. "All other", "All other countries")
    are left exactly as in the source (trimmed), not dropped or altered.
    """
    if not text or not isinstance(text, str):
        return text
    parts = []
    for segment in text.split(','):
        raw = segment.strip()
        if not raw:
            continue
        code = _gogreen_segment_to_code(segment, name_to_code)
        if code:
            parts.append(code)
        else:
            parts.append(raw)
    return ', '.join(parts)


def _apply_gogreen_plus_cost_country_codes(rows, name_to_code):
    """Origin/Destination: country segments → DHL codes; non-country text (e.g. All other) unchanged."""
    for row in rows:
        for key in list(row.keys()):
            if key.lower() not in ('origin', 'destination'):
                continue
            val = row.get(key)
            if isinstance(val, str) and val.strip():
                row[key] = _gogreen_country_list_to_codes(val, name_to_code)


def _fill_country_zoning_country_codes(rows, name_to_code):
    """
    Add a 'Country Code' column to every CountryZoning row by looking up
    the value in the 'Country' column against the name_to_code dictionary.

    After this runs, each row will have a new 'Country Code' field, e.g. "FR" for France.
    Rows where the country name could not be matched will have an empty 'Country Code'.

    At the end, a summary is printed showing how many countries were matched vs missed,
    and a sample of up to 20 unmatched country names (to help diagnose data issues).
    """
    matched = 0
    missing = 0
    missing_countries = []

    for row in rows:
        country = row.get('Country') or ''
        code = _country_to_code(country, name_to_code)
        row['Country Code'] = code

        if country and code:
            matched += 1
        elif country and not code:
            missing += 1
            if len(missing_countries) < 20:
                missing_countries.append(str(country))

    print(f"[*] CountryCode Debug: rows with country matched={matched}, missing={missing}")
    if missing_countries:
        print(f"[WARN] CountryCode Debug: sample missing countries: {missing_countries}")


# ---------------------------------------------------------------------------
# Generic array flattener
# ---------------------------------------------------------------------------

def build_zone_label_lookup(country_zoning):
    """
    Build a lookup table that maps (service_short_prefix, zone_number) -> display label.

    PURPOSE:
    In the MainCosts tab, Origin/Destination values for zoned services are raw zone
    names like "Zone 8".  This function creates a mapping so those raw names can be
    replaced with a meaningful short label that includes the service context, e.g.:
        "Zone 8"  +  service "DHL ECONOMY SELECT EXPORT"
        ->  "ECONOMY_EXP_ZONE_8"

    HOW IT WORKS:
    1. Walk through the raw CountryZoning JSON rows.
    2. For each row that has a non-empty RateName, derive the short prefix using
       _transform_rate_name_to_short() (e.g. "DHL ECONOMY SELECT EXPORT ZONING"
       -> "ECONOMY_EXP_ZONE").
    3. Extract the zone number from the Zone field (e.g. "Zone 8" -> "8").
    4. Store the entry: lookup[(short_prefix, zone_number)] = "ECONOMY_EXP_ZONE_8"

    The lookup is keyed by (short_prefix, zone_number) so that in MainCosts we can:
      - Convert the service name to its short prefix (same _transform_rate_name_to_short logic)
      - Extract the zone number from the Origin/Destination value
      - Look up the display label

    Returns a dict: { (short_prefix, zone_number): label_string, ... }
    e.g. { ("ECONOMY_EXP_ZONE", "8"): "ECONOMY_EXP_ZONE_8", ... }
    """
    lookup = {}
    last_rate_name = ''

    for item in country_zoning:
        rate_name = (item.get('RateName') or '').strip()
        zone = (item.get('Zone') or '').strip()

        if rate_name:
            last_rate_name = rate_name

        effective_rate_name = rate_name or last_rate_name
        if not effective_rate_name or not zone:
            continue

        # Extract the numeric (or letter) part from the zone, e.g. "Zone 8" -> "8", "Zone A" -> "A"
        zone_number = re.sub(r'(?i)^zone\s*', '', zone).strip()
        if not zone_number:
            continue

        # Compute the canonical label from the FULL (unexpanded) rate name.
        # For combined names like "DHL EXPRESS INTERNATIONAL EXPORT ZONING & IMPORT ZONING"
        # this gives "WW_EXP_IMP_ZONE" -> label "WW_EXP_IMP_ZONE_3".
        # For single names like "DHL ECONOMY SELECT EXPORT ZONING" it gives "ECONOMY_EXP_ZONE_3".
        canonical_prefix = _transform_rate_name_to_short(effective_rate_name)
        canonical_label = f"{canonical_prefix}_{zone_number}" if canonical_prefix else None

        # Expand combined rate names joined by " & " into individual variants so we can
        # register the canonical label under each individual service prefix.
        # e.g. "DHL EXPRESS INTERNATIONAL EXPORT ZONING & IMPORT ZONING"
        #   -> ["DHL EXPRESS INTERNATIONAL EXPORT ZONING",
        #       "DHL EXPRESS INTERNATIONAL IMPORT ZONING"]
        expanded_names = _expand_combined_rate_name(effective_rate_name)

        # Transit Times block: the first expanded variant is "DHL EXPRESS Belgium TD International
        # Export" (no "Transit Times"), which would become "WW_EXP" and overwrite (WW_EXP, zone)
        # used by DHL EXPRESS WORLDWIDE EXPORT. So for this block only register under the
        # canonical prefix, never under WW_EXP or WW_IMP.
        is_transit_times = 'TRANSIT TIMES' in (effective_rate_name or '').upper()
        if is_transit_times and canonical_prefix == 'WW_EXP_IMP_TRANSIT_TIMES':
            short_prefix = canonical_prefix
            label = canonical_label or f"{short_prefix}_{zone_number}"
            lookup[(short_prefix, zone_number)] = label
            continue   # skip per-variant registration for this block

        for name_variant in expanded_names:
            short_prefix = _transform_rate_name_to_short(name_variant)
            if not short_prefix:
                continue

            # Use the canonical (combined) label so both EXPORT and IMPORT services
            # get the same zone label (e.g. "WW_EXP_IMP_ZONE_3" for both).
            label = canonical_label or f"{short_prefix}_{zone_number}"

            # Store under the full variant prefix (e.g. "WW_EXP_ZONE", "WW_IMP_ZONE")
            lookup[(short_prefix, zone_number)] = label

            # Also store under the prefix WITHOUT the trailing "_ZONE" so that a service
            # name like "DHL EXPRESS WORLDWIDE EXPORT" (no "ZONING" word) still matches.
            # e.g. "WW_EXP_ZONE" -> also register under "WW_EXP"
            prefix_no_zone = re.sub(r'_ZONE$', '', short_prefix)
            if prefix_no_zone != short_prefix:
                lookup.setdefault((prefix_no_zone, zone_number), label)

    return lookup


def _expand_combined_rate_name(rate_name):
    """
    Split a combined rate name joined by ' & ' into individual name variants.

    Some rate names cover both EXPORT and IMPORT in one string, e.g.:
        "DHL ECONOMY SELECT EXPORT ZONING & IMPORT ZONING"

    This should produce two separate names:
        "DHL ECONOMY SELECT EXPORT ZONING"
        "DHL ECONOMY SELECT IMPORT ZONING"

    HOW IT WORKS:
    Split on ' & '.  The first part is used as-is.  For each subsequent part,
    find the last word in the first part that also appears in the suffix and
    replace from that word onward, effectively substituting the differing tail.

    If there is no ' & ', the original name is returned unchanged in a list.

    Examples:
        "DHL ECONOMY SELECT EXPORT ZONING & IMPORT ZONING"
            -> ["DHL ECONOMY SELECT EXPORT ZONING",
                "DHL ECONOMY SELECT IMPORT ZONING"]

        "DHL EXPRESS WORLDWIDE EXPORT ZONING"
            -> ["DHL EXPRESS WORLDWIDE EXPORT ZONING"]
    """
    parts = [p.strip() for p in rate_name.split(' & ')]
    if len(parts) == 1:
        return parts   # nothing to expand

    base = parts[0].upper()
    results = [base]

    for suffix in parts[1:]:
        suffix_upper = suffix.upper()
        suffix_words = suffix_upper.split()
        if not suffix_words:
            continue

        # Find where to cut the base by looking for the first word of the suffix
        # inside the base.  Cut the base just before that word and append the suffix.
        # e.g. base="DHL ECONOMY SELECT EXPORT ZONING", suffix="IMPORT ZONING"
        #      first suffix word = "IMPORT" — not in base, so try next: "ZONING" — found at idx 4
        #      cut at idx 4 -> "DHL ECONOMY SELECT EXPORT" + "IMPORT ZONING"
        #      -> "DHL ECONOMY SELECT EXPORT IMPORT ZONING"  (wrong — need to cut at EXPORT)
        #
        # Better strategy: find the LAST word in the base that does NOT appear in the suffix.
        # That is the last "unique" base word; cut right after it.
        # e.g. base words: DHL ECONOMY SELECT EXPORT ZONING
        #      suffix words: IMPORT ZONING
        #      "ZONING" is in suffix, "EXPORT" is NOT -> cut after "EXPORT" (idx 3+1=4)
        #      -> "DHL ECONOMY SELECT EXPORT" + "IMPORT ZONING"
        #      -> "DHL ECONOMY SELECT EXPORT IMPORT ZONING"  still wrong...
        #
        # Correct approach: find the first word of the suffix in the base (searching from
        # the right), then cut the base just before that position.
        base_words = base.split()

        # The suffix replaces the last len(suffix_words) words of the base.
        # e.g. base  = "DHL ECONOMY SELECT EXPORT ZONING"  (5 words)
        #      suffix = "IMPORT ZONING"                     (2 words)
        #      cut at 5 - 2 = 3  -> keep "DHL ECONOMY SELECT"
        #      result = "DHL ECONOMY SELECT IMPORT ZONING"
        cut_idx = max(0, len(base_words) - len(suffix_words))

        new_name = ' '.join(base_words[:cut_idx]) + ' ' + suffix_upper
        results.append(new_name.strip())

    return results


def flatten_array_data(array_data, metadata, field_name):
    """
    Convert a JSON array (list of objects) into a list of row dictionaries
    ready to be written to an Excel sheet.

    WHAT IT DOES:
    Each item in the JSON array becomes one row.  Before the item's own fields,
    three common identity columns are prepended to every row:
      - Client       (who the rate card belongs to)
      - Carrier      (which carrier, e.g. DHL Express France)
      - Validity Date (when the rates are valid from)

    SPECIAL HANDLING FOR CountryZoning:
    The CountryZoning data needs two extra enrichment steps that other arrays don't need:
      1. Forward-fill empty RateName cells (see _fill_country_zoning_rate_names)
      2. Add a Country Code column by looking up each country name (see _fill_country_zoning_country_codes)

    SPECIAL HANDLING FOR GoGreenPlusCost:
    Origin and Destination may hold comma-separated entries like "ES - Spain, IT - Italy".
    Recognised countries become ISO codes (e.g. "ES, IT") via dhl_country_codes.txt.
    Segments that are not countries (e.g. "All other", "All other countries") are kept as-is.

    All other arrays (AdditionalZoning, ZoningMatrix, etc.) are passed through as-is
    with just the three identity columns prepended.
    """
    rows = []

    client = (metadata.get('client') or '')
    carrier = (metadata.get('carrier') or '').replace('\n', ' ')
    validity_date = (metadata.get('validity_date') or '')

    for item in array_data:
        row = {
            'Client': client,
            'Carrier': carrier,
            'Validity Date': validity_date
        }
        row.update(item)
        rows.append(row)

    if field_name == 'CountryZoning':
        _fill_country_zoning_rate_names(rows)
        name_to_code = _load_country_codes()
        _fill_country_zoning_country_codes(rows, name_to_code)
    elif field_name == 'GoGreenPlusCost':
        name_to_code = _load_country_codes()
        _apply_gogreen_plus_cost_country_codes(rows, name_to_code)

    return rows


# ---------------------------------------------------------------------------
# AddedRates pivot
# ---------------------------------------------------------------------------

def _is_added_rates_header_row(item):
    """
    Decide whether a single AddedRates JSON item is a "header row" or a "data row".

    BACKGROUND:
    The AddedRates JSON mixes two types of rows together in one flat list:
      - Header rows: contain the zone names (e.g. "Zone 1", "Zone 2") in the Zone1, Zone2 … fields.
                     They also carry the table name and page reference.
      - Data rows:   contain actual weight ranges and prices (e.g. WeightFrom=0.5, Zone1=12.50).

    We detect header rows by checking:
      - WeightFrom == "From"  (the literal word "From" signals a header, not a weight value)
      - OR Zone1 value starts with "Zone"  (the cell contains a zone label, not a price)

    Returns True if this item is a header row, False if it is a data row.
    """
    weight_from = item.get('WeightFrom', '')
    zone1_val = item.get('Zone1', '')
    if weight_from == 'From' or (str(zone1_val).strip().startswith('Zone')):
        return True
    return False


def pivot_added_rates(added_rates, metadata):
    """
    Convert the AddedRates JSON list into a clean flat table for Excel.

    THE CHALLENGE:
    The source JSON for AddedRates looks like this (simplified):
        { WeightFrom:"From", WeightTo:"To", Zone1:"Zone 1", Zone2:"Zone 2", TableName:"Fuel Surcharge" }  <- header
        { WeightFrom:"0",    WeightTo:"0.5", Zone1:"12.50", Zone2:"14.00" }                              <- data
        { WeightFrom:"0.5",  WeightTo:"1",   Zone1:"15.00", Zone2:"17.50" }                              <- data
        { WeightFrom:"From", WeightTo:"To", Zone1:"Zone 1", Zone2:"Zone 2", TableName:"Remote Area" }    <- header (new table)
        ...

    Header rows tell us what the zone columns are called.
    Data rows contain the actual weight ranges and prices.

    WHAT THIS FUNCTION PRODUCES:
    Every JSON item becomes one output row.  For data rows, the Zone1, Zone2 … values
    are written under the human-readable column names taken from the most recent header row.
    Page Stopper and Table Name are only filled on header rows (they are blank on data rows).

    Example output:
        Client | Carrier | Validity Date | Page Stopper | Table Name     | Weight From | Weight To | Zone 1 | Zone 2
               |         |               | p.5          | Fuel Surcharge | From        | To        | Zone 1 | Zone 2   <- header row
               |         |               |              |                | 0           | 0.5       | 12.50  | 14.00    <- data row
               |         |               |              |                | 0.5         | 1         | 15.00  | 17.50    <- data row
    """
    rows = []
    client = (metadata.get('client') or '')
    carrier = (metadata.get('carrier') or '').replace('\n', ' ')
    validity_date = (metadata.get('validity_date') or '')

    # zone_column_names holds the current mapping from JSON key to display label.
    # e.g. [("Zone1", "Zone 1"), ("Zone2", "Zone 2")]
    # It is rebuilt every time we encounter a new header row.
    zone_column_names = []
    current_table_name = ''
    current_page_stopper = ''

    for item in added_rates:
        is_header = _is_added_rates_header_row(item)

        if is_header:
            # This is a header row — capture the zone column names for the data rows
            # that follow, then skip it (don't write it as a data row in the output).
            zone_column_names = []
            zone_keys = [k for k in item.keys() if k.startswith('Zone')]

            # Sort the zone keys numerically: Zone1, Zone2, Zone3 … (not Zone1, Zone10, Zone2)
            def zone_sort_key(k):
                suffix = k[4:]   # the number part after "Zone", e.g. "1" from "Zone1"
                try:
                    return int(suffix)
                except ValueError:
                    return 0

            for k in sorted(zone_keys, key=zone_sort_key):
                zone_column_names.append((k, str(item.get(k, k)).strip() or k))

            # Also capture Table Name from the header so data rows can carry it
            current_table_name = item.get('TableName', '')
            current_page_stopper = item.get('PageStopper', '')
            continue   # skip writing the header row itself to the output

        # Build the output row for data rows only
        weight_from = item.get('WeightFrom', '')
        weight_to = item.get('WeightTo', '')
        row = {
            'Client': client,
            'Carrier': carrier,
            'Validity Date': validity_date,
            'Page Stopper': current_page_stopper,
            'Table Name': current_table_name,
            'Weight From': weight_from,
            'Weight To': weight_to,
        }

        # For each zone column, read the value from the JSON item using the internal key
        # (e.g. "Zone1") but write it under the human-readable display label (e.g. "Zone 1").
        for zone_key, zone_label in zone_column_names:
            row[zone_label] = item.get(zone_key, '')

        rows.append(row)

    return rows
