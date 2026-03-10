import pandas as pd
import os
import json
import re

NEW_STRUCTURE_FILE = os.getenv('PERU_FILE')
OUTPUT_DIR = "json/"
OUTPUT_DIR_LATEST = "json/latest/"


def get_version_from_excel(filepath):
    """
    Read version from 'version' sheet, cell B1. Format: x.x.x
    Returns the version string if valid, raises ValueError if invalid.
    """
    try:
        version_df = pd.read_excel(filepath, sheet_name="version", header=None)
        version_str = str(version_df.iloc[0, 1]).strip()  # B1 = row 0, col 1

        if not re.match(r'^(\d+)\.(\d+)\.(\d+)$', version_str):
            raise ValueError(f"Invalid version format: {version_str}")

        return version_str
    except Exception as e:
        raise ValueError(f"Failed to read version from Excel: {e}")

MISSING_VOTE_DEFAULT = 0.5
MISSING_COMMENT_DEFAULT = "No se encontró información pública sobre su posición."
MISSING_SOURCE_DEFAULT = None

number_of_topics = 20

def text_to_key(text):
    """Spanish text -> snake_case key"""
    if text is None:
        return None
    text = str(text).strip().lower()
    text = re.sub(r'[^\w\sáéíóúñü]', '', text, flags=re.UNICODE)
    text = re.sub(r'\s+', '_', text)
    text = text.strip('_')
    return text

def build_comment_key(entity_type, entity_name, topic_key):
    """explanations.{type}.{entity}.{topic}"""
    if not entity_name or not topic_key:
        return None
    entity_key = text_to_key(entity_name)
    topic_part = topic_key.replace("topics.", "") if topic_key.startswith("topics.") else text_to_key(topic_key)
    return f"explanations.{entity_type}.{entity_key}.{topic_part}"

def clean_text(s):
    # Handle pandas objects that are not scalars (Series/DataFrame)
    if isinstance(s, pd.Series):
        # if it's a 1-element series, unwrap; otherwise treat as missing (or raise)
        if len(s) == 1:
            s = s.iloc[0]
        else:
            return None
    if isinstance(s, pd.DataFrame):
        return None

    if s is None:
        return None

    # pd.isna works fine for scalars (including numpy.nan)
    if pd.isna(s):
        return None
    s = str(s).strip()
    return s if s != "" else None

def map_vote_text_to_value(vote_text):
    """'A favor' -> 1.0, 'En contra' -> 0.0, 'Neutral' -> 0.5"""
    if vote_text is None:
        return None

    vt = str(vote_text).strip()
    if vt == "":
        return None

    try:
        num = float(vt.replace(',', '.'))
        return num
    except Exception:
        pass

    vt_low = vt.lower()

    if "a favor" in vt_low or vt_low == "favor":
        return 1.0
    if "en contra" in vt_low or "contra" == vt_low:
        return 0.0
    if "neutral" in vt_low:
        return 0.5
    if vt_low in ("sí", "si", "yes"):
        return 1.0
    if vt_low in ("no",):
        return 0.0

    return None

def load_structure_sheet(filepath, sheet_name, number_of_topics):
    raw_df = pd.read_excel(
        filepath,
        sheet_name=sheet_name,
        dtype=str,
        header=None
    )

    raw_df = raw_df.head(number_of_topics + 1)
    if raw_df.shape[0] < 1:
        raise ValueError(f"Input sheet '{sheet_name}' appears empty or is missing the header row.")

    raw_df.columns = raw_df.iloc[0]
    df = raw_df.drop(index=0).reset_index(drop=True)

    if "Statement" not in df.columns or "Tema" not in df.columns:
        raise ValueError(f"Expected 'Tema' and 'Statement' columns in sheet '{sheet_name}'.")

    return df


def extract_party_from_candidate_header(col_name):
    if col_name is None:
        return None
    s = str(col_name).strip()
    m = re.search(r"\(([^)]+)\)\s*$", s)
    return m.group(1).strip() if m else None


def build_presidential_party_column_map(pres_df):
    """
    Map normalized party key -> presidential column name
    """
    pres_cols = [c for c in pres_df.columns if c not in ("Tema", "Statement")]
    party_to_col = {}
    for c in pres_cols:
        party = extract_party_from_candidate_header(c)
        if party:
            party_to_col[text_to_key(party)] = c
    return party_to_col

def parse_cell_combined(cell_value):
    """Parse 'vote+++comment+++source' format. Returns (None,None,None) if empty."""
    raw = clean_text(cell_value)
    if raw is None:
        return None, None, None

    parts = raw.split('+++', 2)
    vote_part = clean_text(parts[0]) if len(parts) >= 1 else None
    comment_part = clean_text(parts[1]) if len(parts) >= 2 else None
    source_part = clean_text(parts[2]) if len(parts) >= 3 else None

    vote_mapped = map_vote_text_to_value(vote_part)
    return vote_mapped, comment_part, source_part

def generate_from_new_structure():
    # Load both sheets
    parl_df = load_structure_sheet(NEW_STRUCTURE_FILE, "parlamentaria", number_of_topics)
    pres_df = load_structure_sheet(NEW_STRUCTURE_FILE, "presidencial", number_of_topics)

    # Identify party columns in parlamentaria
    party_columns = [col for col in parl_df.columns if col not in ("Tema", "Statement")]

    # Build presidential mapping: party -> candidate column
    pres_party_to_col = build_presidential_party_column_map(pres_df)

    # Build a fast lookup from (tema, statement) -> row index in presidencial
    # This assumes the same "Tema"/"Statement" values exist in both sheets.
    pres_index = {}
    for i in range(pres_df.shape[0]):
        t = clean_text(pres_df.at[i, "Tema"])
        s = clean_text(pres_df.at[i, "Statement"])
        if s is None:
            continue
        pres_index[(t, s)] = i

    parties_info = {}
    for party_column in party_columns:
        parties_info[party_column] = {
            "header": party_column,
            "name": party_column,
            "votes": {}
        }

    # Iterate parlamentaria rows
    for row_index in range(parl_df.shape[0]):
        topic_text = clean_text(parl_df.at[row_index, "Tema"])
        statement_text = clean_text(parl_df.at[row_index, "Statement"])

        if statement_text is None:
            continue

        question_identifier = f"{topic_text}: {statement_text}" if topic_text else statement_text
        question_key = f"questions.{text_to_key(statement_text)}"
        topic_key = f"topics.{text_to_key(topic_text)}" if topic_text else None

        for party_column in party_columns:
            # 1) Try parlamentaria cell
            cell_value = parl_df.at[row_index, party_column] if party_column in parl_df.columns else None
            vote_value, comment_value, source_value = parse_cell_combined(cell_value)

            # 2) If missing, fallback to presidential candidate column for that party
            if vote_value is None and comment_value is None and source_value is None:
                party_key = text_to_key(party_column)  # normalize party from parlamentaria header
                pres_col = pres_party_to_col.get(party_key)

                pres_row = pres_index.get((topic_text, statement_text))
                if pres_col and pres_row is not None and pres_col in pres_df.columns:
                    pres_cell_value = pres_df.at[pres_row, pres_col]
                    vote_value, comment_value, source_value = parse_cell_combined(pres_cell_value)

            # 3) If still missing, SKIP this statement for this party (do not write defaults)
            if vote_value is None and comment_value is None and source_value is None:
                continue

            comment_key = None
            if comment_value:
                comment_key = build_comment_key("party", party_column, topic_key)

            parties_info[party_column]["votes"][question_identifier] = {
                "tema": topic_text,
                "question": statement_text,
                "question_key": question_key,
                "topic_key": topic_key,
                "vote": vote_value,
                "comment": comment_value,
                "comment_key": comment_key,
                "source": source_value
            }

    # Get version from Excel
    try:
        version = get_version_from_excel(NEW_STRUCTURE_FILE)
        print(f"Found version: {version}")
    except ValueError as e:
        print(f"Warning: {e}")
        version = None

    combined_output = {"parties": {}}
    if version:
        combined_output["version"] = version

    for party_column, pinfo in parties_info.items():
        combined_output["parties"][pinfo["header"]] = {
            "name": pinfo["name"],
            "votes": pinfo["votes"]
        }

    # Ensure output directories exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR_LATEST, exist_ok=True)

    # Write to main output directory
    output_path = os.path.join(OUTPUT_DIR, "combined_votes_peru_partidos_2026.json")
    with open(output_path, "w", encoding="utf-8") as file_handle:
        json.dump(combined_output, file_handle, ensure_ascii=False, indent=2)
    print(f"Wrote {output_path}")

    # Write versioned file to latest directory
    if version:
        versioned_filename = f"combined_votes_peru_partidos_2026_v{version}.json"
        versioned_path = os.path.join(OUTPUT_DIR_LATEST, versioned_filename)
        with open(versioned_path, "w", encoding="utf-8") as file_handle:
            json.dump(combined_output, file_handle, ensure_ascii=False, indent=2)
        print(f"Wrote {versioned_path}")

if __name__ == "__main__":
    generate_from_new_structure()
