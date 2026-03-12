import pandas as pd
import os
import json
import re

NEW_STRUCTURE_FILE = os.getenv("PERU_FILE")

OUTPUT_DIR_LATEST = "json/latest/"
OUTPUT_DIR_HISTORY = "json/history/"

number_of_topics = 20


def get_version_from_excel(filepath):
    """
    Read version from 'version' sheet, cell B1. Format: x.x.x
    Returns the version string if valid, raises ValueError if invalid.
    """
    try:
        version_df = pd.read_excel(filepath, sheet_name="version", header=None)
        version_str = str(version_df.iloc[0, 1]).strip()  # B1 = row 0, col 1

        if not re.match(r"^(\d+)\.(\d+)\.(\d+)$", version_str):
            raise ValueError(f"Invalid version format: {version_str}")

        return version_str
    except Exception as e:
        raise ValueError(f"Failed to read version from Excel: {e}")


def text_to_key(text):
    """Spanish text -> snake_case key"""
    if text is None:
        return None
    text = str(text).strip().lower()
    text = re.sub(r"[^\w\sáéíóúñü]", "", text, flags=re.UNICODE)
    text = re.sub(r"\s+", "_", text)
    text = text.strip("_")
    return text


def clean_text(s):
    # Unwrap 1-element Series/ndarray; reject multi-element objects
    if isinstance(s, pd.Series):
        if len(s) == 1:
            s = s.iloc[0]
        else:
            return None
    elif isinstance(s, pd.DataFrame):
        return None

    # numpy arrays can appear sometimes; treat similarly
    try:
        import numpy as np
        if isinstance(s, np.ndarray):
            if s.size == 1:
                s = s.item()
            else:
                return None
    except Exception:
        pass

    if s is None:
        return None

    if pd.isna(s):
        return None

    s = str(s).strip()
    return s if s != "" else None


def normalize_id(value):
    """
    Normalize an ID coming from ID_tema / ID_candidate / ID_party.
    Keeps the spreadsheet value as the source of truth, but normalizes
    to a safe key format.
    """
    cleaned = clean_text(value)
    if cleaned is None:
        return None
    return text_to_key(cleaned)


def build_topic_key_from_id(id_tema_value):
    base_id = normalize_id(id_tema_value)
    if not base_id:
        return None
    return f"topics.{base_id}"


def build_question_key_from_id(id_tema_value):
    base_id = normalize_id(id_tema_value)
    if not base_id:
        return None
    return f"questions.{base_id}"


def build_comment_key(entity_type, entity_id, question_key):
    """
    explanations.{entity_type}.{entity_id}.{question_part}

    Example:
      explanations.parties.p1.t1
      explanations.candidates.c1.t1
    """
    entity_id_key = normalize_id(entity_id)
    if not entity_id_key or not question_key:
        return None

    question_part = question_key.replace("questions.", "") if question_key.startswith("questions.") else text_to_key(question_key)
    return f"explanations.{entity_type}.{entity_id_key}.{question_part}"


def parse_candidate_header(header_str):
    """'Name (Party)' -> (name, party)"""
    if header_str is None:
        return None, None
    header_str = str(header_str).strip()
    m = re.match(r"^(.*?)\s*\((.*?)\)\s*$", header_str)
    if m:
        name = m.group(1).strip()
        party = m.group(2).strip()
    else:
        name = header_str
        party = None
    return name, party


def map_vote_text_to_value(vote_text):
    """'A favor' -> 1.0, 'En contra' -> 0.0, 'Neutral' -> 0.5"""
    if vote_text is None:
        return None

    vt = str(vote_text).strip()
    if vt == "":
        return None

    try:
        num = float(vt.replace(",", "."))
        return num
    except Exception:
        pass

    vt_low = vt.lower()

    if "a favor" in vt_low or vt_low == "favor":
        return 1.0
    if "en contra" in vt_low or vt_low == "contra":
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

    # header row + up to number_of_topics rows + 1 extra metadata row
    # (ID_candidate in presidencial or ID_party in parlamentaria)
    if number_of_topics is not None:
        raw_df = raw_df.head(number_of_topics + 2)

    if raw_df.shape[0] < 1:
        raise ValueError(f"Input sheet '{sheet_name}' appears empty or is missing the header row.")

    raw_df.columns = raw_df.iloc[0]
    df = raw_df.drop(index=0).reset_index(drop=True)

    required_columns = {"ID_tema", "Tema", "Statement"}
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(
            f"Expected columns {sorted(required_columns)} in sheet '{sheet_name}'. "
            f"Missing: {sorted(missing_columns)}"
        )

    return df


def get_entity_ids_from_special_row(df, special_row_id, excluded_columns=None):
    """
    Reads a metadata row identified by ID_tema == special_row_id and returns:
      {column_name: normalized_id_value}
    """
    if excluded_columns is None:
        excluded_columns = {"ID_tema", "Tema", "Statement"}

    special_row_mask = df["ID_tema"].astype(str).str.strip().str.lower() == special_row_id.lower()
    special_rows = df[special_row_mask]

    if special_rows.empty:
        return {}

    row = special_rows.iloc[0]
    entity_ids = {}

    for col in df.columns:
        if col in excluded_columns:
            continue
        entity_ids[col] = normalize_id(row[col])

    return entity_ids


def build_party_column_map(parl_df):
    """
    Map normalized party key -> actual party column name in parlamentaria.
    parlamentaria columns are party names (no candidate name).
    """
    excluded_columns = {"ID_tema", "Tema", "Statement"}
    party_cols = [c for c in parl_df.columns if c not in excluded_columns]
    return {text_to_key(c): c for c in party_cols}


def build_party_id_map(parl_df):
    """
    Map normalized party key (derived from column name) -> party_id
    from the ID_party row in parlamentaria.
    """
    excluded_columns = {"ID_tema", "Tema", "Statement"}
    party_ids_by_column = get_entity_ids_from_special_row(parl_df, "ID_party", excluded_columns=excluded_columns)

    party_key_to_id = {}
    for col, party_id in party_ids_by_column.items():
        party_key_to_id[text_to_key(col)] = party_id

    return party_key_to_id, party_ids_by_column


def parse_cell_combined(cell_value):
    """Parse 'vote+++comment+++source' format. Returns (None,None,None) if empty."""
    raw = clean_text(cell_value)
    if raw is None:
        return None, None, None

    parts = raw.split("+++", 2)
    vote_part = clean_text(parts[0]) if len(parts) >= 1 else None
    comment_part = clean_text(parts[1]) if len(parts) >= 2 else None
    source_part = clean_text(parts[2]) if len(parts) >= 3 else None

    vote_mapped = map_vote_text_to_value(vote_part)
    return vote_mapped, comment_part, source_part


def is_metadata_row(id_tema_value):
    normalized = normalize_id(id_tema_value)
    return normalized in {"id_candidate", "id_party"}


def generate_from_new_structure():
    # Load both sheets
    pres_df = load_structure_sheet(NEW_STRUCTURE_FILE, "presidencial", number_of_topics)
    parl_df = load_structure_sheet(NEW_STRUCTURE_FILE, "parlamentaria", number_of_topics)

    excluded_columns = {"ID_tema", "Tema", "Statement"}

    # Candidate columns in presidencial
    candidate_columns = [col for col in pres_df.columns if col not in excluded_columns]

    # Candidate IDs from special row in presidencial
    candidate_ids_by_column = get_entity_ids_from_special_row(
        pres_df,
        "ID_candidate",
        excluded_columns=excluded_columns
    )

    # Party mapping and party IDs from parlamentaria
    party_to_parl_col = build_party_column_map(parl_df)
    party_key_to_id, party_ids_by_column = build_party_id_map(parl_df)

    # Build candidate metadata
    candidates_info = {}
    for candidate_column in candidate_columns:
        candidate_name, candidate_party = parse_candidate_header(candidate_column)
        candidate_party_key = text_to_key(candidate_party) if candidate_party else None
        candidate_id = candidate_ids_by_column.get(candidate_column)
        party_id = party_key_to_id.get(candidate_party_key) if candidate_party_key else None

        candidates_info[candidate_column] = {
            "id": candidate_id,
            "header": candidate_column,
            "name": candidate_name,
            "party": candidate_party,
            "party_id": party_id,
            "votes": {}
        }

    # Row index lookups by ID_tema for both sheets
    pres_index = {}
    for i in range(pres_df.shape[0]):
        id_tema = clean_text(pres_df.at[i, "ID_tema"])
        statement = clean_text(pres_df.at[i, "Statement"])

        if id_tema is None or statement is None:
            continue
        if is_metadata_row(id_tema):
            continue

        pres_index[normalize_id(id_tema)] = i

    parl_index = {}
    for i in range(parl_df.shape[0]):
        id_tema = clean_text(parl_df.at[i, "ID_tema"])
        statement = clean_text(parl_df.at[i, "Statement"])

        if id_tema is None or statement is None:
            continue
        if is_metadata_row(id_tema):
            continue

        parl_index[normalize_id(id_tema)] = i

    # Iterate over presidential statements using pres_df as the driver
    for id_tema_base, pres_row in pres_index.items():
        id_tema_raw = clean_text(pres_df.at[pres_row, "ID_tema"])
        topic_text = clean_text(pres_df.at[pres_row, "Tema"])
        statement_text = clean_text(pres_df.at[pres_row, "Statement"])

        if statement_text is None:
            continue

        question_identifier = f"{topic_text}: {statement_text}" if topic_text else statement_text
        topic_key = build_topic_key_from_id(id_tema_raw)
        question_key = build_question_key_from_id(id_tema_raw)

        for candidate_column in candidate_columns:
            candidate_meta = candidates_info[candidate_column]
            candidate_id = candidate_meta["id"]
            party_id = candidate_meta["party_id"]

            # 1) Try candidate cell in presidencial
            cell_value = pres_df.at[pres_row, candidate_column]
            vote_value, comment_value, source_value = parse_cell_combined(cell_value)

            value_origin = None
            comment_key = None

            if vote_value is not None or comment_value is not None or source_value is not None:
                value_origin = "candidate"
                if comment_value:
                    comment_key = build_comment_key("candidates", candidate_id, question_key)

            # 2) If missing, fallback to the candidate's party cell in parlamentaria
            if vote_value is None and comment_value is None and source_value is None:
                candidate_party = candidate_meta["party"]
                candidate_party_key = text_to_key(candidate_party) if candidate_party else None
                parl_col = party_to_parl_col.get(candidate_party_key) if candidate_party_key else None
                parl_row = parl_index.get(id_tema_base)

                if parl_col and parl_row is not None and parl_col in parl_df.columns:
                    party_cell_value = parl_df.at[parl_row, parl_col]
                    vote_value, comment_value, source_value = parse_cell_combined(party_cell_value)

                    if vote_value is not None or comment_value is not None or source_value is not None:
                        value_origin = "party_fallback"
                        if comment_value:
                            comment_value = (
                                "Por falta de información, se tomó la posición del partido: "
                                f"{comment_value}"
                            )
                            comment_key = build_comment_key("candidates", candidate_id, question_key)

            # 3) If still missing, SKIP this statement for this candidate
            if vote_value is None and comment_value is None and source_value is None:
                continue

            candidate_meta["votes"][question_identifier] = {
                "id_tema": id_tema_raw,
                "tema": topic_text,
                "question": statement_text,
                "question_key": question_key,
                "topic_key": topic_key,
                "vote": vote_value,
                "comment": comment_value,
                "comment_key": comment_key,
                "source": source_value,
                "source_type": value_origin
            }

    # Get version from Excel
    try:
        version = get_version_from_excel(NEW_STRUCTURE_FILE)
        print(f"Found version: {version}")
    except ValueError as e:
        print(f"Warning: {e}")
        version = None

    combined_output = {"candidates": {}}
    if version:
        combined_output["version"] = version

    for candidate_column, candidate_info in candidates_info.items():
        candidate_output_key = candidate_info["id"] if candidate_info["id"] else candidate_info["header"]

        combined_output["candidates"][candidate_output_key] = {
            "name": candidate_info["name"],
            "party": candidate_info["party"],
            "header": candidate_info["header"],
            "votes": candidate_info["votes"]
        }

    # Ensure output directories exist
    os.makedirs(OUTPUT_DIR_LATEST, exist_ok=True)

    # Write to latest directory (always replaced, no version suffix)
    latest_path = os.path.join(OUTPUT_DIR_LATEST, "combined_votes_peru_pres_2026.json")
    with open(latest_path, "w", encoding="utf-8") as file_handle:
        json.dump(combined_output, file_handle, ensure_ascii=False, indent=2)
    print(f"Wrote {latest_path}")

    # Write to history directory (versioned folder with versioned filename)
    if version:
        version_underscored = version.replace('.', '_')
        version_folder = f"v{version_underscored}"
        history_version_dir = os.path.join(OUTPUT_DIR_HISTORY, version_folder)
        os.makedirs(history_version_dir, exist_ok=True)

        history_filename = f"combined_votes_peru_pres_2026_{version_underscored}.json"
        history_path = os.path.join(history_version_dir, history_filename)
        with open(history_path, "w", encoding="utf-8") as file_handle:
            json.dump(combined_output, file_handle, ensure_ascii=False, indent=2)
        print(f"Wrote {history_path}")


if __name__ == "__main__":
    generate_from_new_structure()