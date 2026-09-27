import pandas as pd
import os
import json
import re

NEW_STRUCTURE_FILE = os.getenv("PERU_REGIONS_FILE")

OUTPUT_DIR_LATEST = "json/regions/latest/"
OUTPUT_DIR_HISTORY = "json/regions/history/"


def topic_sort_key(topic_id):
    """
    Sort key for topic IDs: 'PE' (common) topics always first, then the
    rest in natural order, e.g. PE1, PE2, ..., PE10, L1, L2, ..., L10.
    """
    topic_id = topic_id or ""
    match = re.match(r"^([A-Za-z]+)(\d+)$", topic_id)
    prefix, number = (match.group(1), int(match.group(2))) if match else (topic_id, 0)
    return (0 if prefix == "PE" else 1, prefix, number)

NON_REGION_SHEETS = {"version", "tesis"}


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

    if s is None:
        return None

    if pd.isna(s):
        return None

    s = str(s).strip()
    return s if s != "" else None


def normalize_id(value):
    """Normalize IDs from Excel so they are safe and consistent in keys."""
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


def parse_candidate_header(header_str):
    """'Name(Party)' -> (name, party). Handles parties with nested parentheses."""
    if header_str is None:
        return None, None
    header_str = str(header_str).strip()
    m = re.match(r"^(.*?)\s*\((.*)\)\s*$", header_str)
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


def get_region_sheet_names(filepath):
    """Every sheet except 'version' and 'tesis' is a region sheet."""
    xl = pd.ExcelFile(filepath)
    return [name for name in xl.sheet_names if name not in NON_REGION_SHEETS]


PRIORITY_REGION = "Lima Metropolitana"


def assign_region_codes(region_names):
    """
    Assign R1, R2, ... codes to regions. 'Lima Metropolitana' is always R1;
    the rest are ordered alphabetically.
    Returns a list of (region_code, region_name) tuples, in code order.
    """
    remaining = sorted(
        (name for name in region_names if name != PRIORITY_REGION),
        key=lambda s: s.lower(),
    )

    ordered_names = []
    if PRIORITY_REGION in region_names:
        ordered_names.append(PRIORITY_REGION)
    ordered_names.extend(remaining)

    return [(f"r{i}", name) for i, name in enumerate(ordered_names, start=1)]


def load_region_sheet(filepath, sheet_name):
    raw_df = pd.read_excel(filepath, sheet_name=sheet_name, dtype=str, header=None)

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


def build_region_output(df):
    """Build the {'candidates': {...}} block for a single region sheet."""
    excluded_columns = {"ID_tema", "Tema", "Statement"}
    candidate_columns = [
        col
        for col in df.columns
        if col not in excluded_columns and clean_text(col) is not None
    ]

    # No ID_candidate metadata row exists in these sheets (unlike presidencial/
    # parlamentaria), so candidate codes are auto-generated here: c1..cN in
    # column order, resetting for each region sheet.
    candidate_codes = {
        candidate_column: f"c{i}"
        for i, candidate_column in enumerate(candidate_columns, start=1)
    }

    candidates_info = {}
    for candidate_column in candidate_columns:
        candidate_id = candidate_codes[candidate_column]
        candidate_name, candidate_party = parse_candidate_header(candidate_column)
        candidates_info[candidate_id] = {
            "id": candidate_id,
            "name": candidate_name,
            "party": candidate_party,
            "header": candidate_column,
            "votes": {},
        }

    for row_index in range(df.shape[0]):
        id_tema_raw = clean_text(df.at[row_index, "ID_tema"])
        topic_text = clean_text(df.at[row_index, "Tema"])
        statement_text = clean_text(df.at[row_index, "Statement"])

        if id_tema_raw is None or statement_text is None:
            continue

        question_identifier = f"{topic_text}: {statement_text}" if topic_text else statement_text
        topic_key = build_topic_key_from_id(id_tema_raw)
        question_key = build_question_key_from_id(id_tema_raw)

        for candidate_column in candidate_columns:
            candidate_id = candidate_codes[candidate_column]
            candidate_meta = candidates_info[candidate_id]

            cell_value = df.at[row_index, candidate_column]
            vote_value, comment_value, source_value = parse_cell_combined(cell_value)

            if vote_value is None and comment_value is None and source_value is None:
                continue

            question_part = question_key.replace("questions.", "") if question_key else None
            comment_key = (
                f"explanations.candidates.{candidate_id}.{question_part}"
                if comment_value and question_part
                else None
            )

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
                "source_type": "candidate",
            }

    for candidate_meta in candidates_info.values():
        candidate_meta["votes"] = dict(
            sorted(
                candidate_meta["votes"].items(),
                key=lambda kv: topic_sort_key(kv[1]["id_tema"]),
            )
        )

    return {"candidates": candidates_info}


def generate_from_new_structure():
    region_names = get_region_sheet_names(NEW_STRUCTURE_FILE)
    region_codes = assign_region_codes(region_names)

    regions_output = {}
    for region_code, region_name in region_codes:
        region_df = load_region_sheet(NEW_STRUCTURE_FILE, region_name)
        region_data = build_region_output(region_df)
        regions_output[region_code] = {
            "name": region_name,
            "candidates": region_data["candidates"],
        }

    try:
        version = get_version_from_excel(NEW_STRUCTURE_FILE)
        print(f"Found version: {version}")
    except ValueError as e:
        print(f"Warning: {e}")
        version = None

    combined_output = {"regions": regions_output}
    if version:
        combined_output["version"] = version

    os.makedirs(OUTPUT_DIR_LATEST, exist_ok=True)

    latest_path = os.path.join(OUTPUT_DIR_LATEST, "combined_votes_peru_regions_2026.json")
    with open(latest_path, "w", encoding="utf-8") as file_handle:
        json.dump(combined_output, file_handle, ensure_ascii=False, indent=2)
    print(f"Wrote {latest_path}")

    if version:
        version_underscored = version.replace(".", "_")
        version_folder = f"v{version_underscored}"
        history_version_dir = os.path.join(OUTPUT_DIR_HISTORY, version_folder)
        os.makedirs(history_version_dir, exist_ok=True)

        history_filename = f"combined_votes_peru_regions_2026_{version_underscored}.json"
        history_path = os.path.join(history_version_dir, history_filename)
        with open(history_path, "w", encoding="utf-8") as file_handle:
            json.dump(combined_output, file_handle, ensure_ascii=False, indent=2)
        print(f"Wrote {history_path}")


if __name__ == "__main__":
    generate_from_new_structure()
