import pandas as pd
import os
import json
import re

NEW_STRUCTURE_FILE = os.getenv("PERU_FILE")
NEW_STRUCTURE_FILE = r"C:\Users\josev\OneDrive\Proyectos\Votometro\Peru\Votaciones parlamentarias 2026\peru_preguntas_20250715.xlsx"

OUTPUT_DIR = "json/"
OUTPUT_DIR_LATEST = "json/latest/"

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

def party_id_to_candidate_id(party_id):
    """
    Convert parlamentaria party ID like 'p37' to presidencial candidate ID like 'c37'.
    """
    pid = normalize_id(party_id)
    if not pid:
        return None

    m = re.fullmatch(r"p(\d+)", pid)
    if not m:
        return None

    return f"c{m.group(1)}"

def clean_text(s):
    # Handle pandas objects that are not scalars (Series/DataFrame)
    if isinstance(s, pd.Series):
        if len(s) == 1:
            s = s.iloc[0]
        else:
            return None
    if isinstance(s, pd.DataFrame):
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

def build_party_id_to_column_map(df, metadata_row_name="ID_party"):
    row = get_row_by_id_tema(df, metadata_row_name)
    if row is None:
        return {}

    entity_columns = get_entity_columns(df)
    out = {}
    for col in entity_columns:
        entity_id = normalize_id(row.get(col))
        if entity_id:
            out[entity_id] = col
    return out

def build_question_key_from_id(id_tema_value):
    """
    Example:
      ID_tema = 'seguridad'
      question_key = 'questions.seguridad'
      topic_key    = 'topics.seguridad'
    """
    base_id = normalize_id(id_tema_value)
    if not base_id:
        return None, None

    question_key = f"questions.{base_id}"
    topic_key = f"topics.{base_id}"
    return question_key, topic_key

def build_candidate_id_to_column_map(df):
    row = get_row_by_id_tema(df, "ID_candidate")
    if row is None:
        raise ValueError("Missing 'ID_candidate' row in presidencial sheet.")

    entity_columns = get_entity_columns(df)
    out = {}
    for col in entity_columns:
        candidate_id = normalize_id(row.get(col))
        if candidate_id:
            out[candidate_id] = col

    return out

def build_comment_key(entity_type, entity_id, question_key):
    """
    explanations.{entity_type}.{entity_id}.{question_id_question}
    Example:
      explanations.party.apra.seguridad_question
      explanations.candidate.keiko_fujimori.seguridad_question
    """
    entity_id_norm = normalize_id(entity_id)
    if not entity_id_norm or not question_key:
        return None

    question_part = question_key.replace("questions.", "") if question_key.startswith("questions.") else normalize_id(question_key)
    if not question_part:
        return None

    return f"explanations.{entity_type}.{entity_id_norm}.{question_part}"


def load_structure_sheet(filepath, sheet_name, number_of_topics):
    """
    Reads the sheet and preserves all metadata rows.

    Expected first row in the Excel sheet: headers.
    Expected first column header: ID_tema
    Other expected columns include: Tema, Statement, plus entity columns.
    """
    raw_df = pd.read_excel(
        filepath,
        sheet_name=sheet_name,
        dtype=str,
        header=None
    )

    if raw_df.shape[0] < 1:
        raise ValueError(f"Input sheet '{sheet_name}' appears empty or is missing the header row.")

    raw_df.columns = raw_df.iloc[0]
    df = raw_df.drop(index=0).reset_index(drop=True)

    if "ID_tema" not in df.columns:
        raise ValueError(f"Expected 'ID_tema' column in sheet '{sheet_name}'.")
    if "Statement" not in df.columns or "Tema" not in df.columns:
        raise ValueError(f"Expected 'Tema' and 'Statement' columns in sheet '{sheet_name}'.")

    # Keep the metadata rows too (ID_party / ID_candidate), but limit question rows later.
    return df


def get_row_by_id_tema(df, row_id):
    """
    Return the first row whose ID_tema equals row_id, normalized.
    """
    row_id_norm = normalize_id(row_id)
    matches = df[df["ID_tema"].apply(normalize_id) == row_id_norm]
    if matches.empty:
        return None
    return matches.iloc[0]


def get_question_rows(df, max_questions):
    """
    Return only the real question rows, excluding metadata rows such as ID_party / ID_candidate.
    Limit to max_questions question rows.
    """
    metadata_ids = {"id_party", "id_candidate"}

    question_df = df[~df["ID_tema"].apply(normalize_id).isin(metadata_ids)].copy()
    question_df = question_df.head(max_questions).reset_index(drop=True)
    return question_df


def get_entity_columns(df):
    """
    Entity columns are all columns except the known structural columns.
    """
    excluded = {"ID_tema", "Tema", "Statement"}
    return [col for col in df.columns if col not in excluded]


def build_entity_id_map(df, metadata_row_name):
    """
    Build map: column_name -> entity_id
    using the row whose ID_tema == metadata_row_name.

    Example:
      metadata_row_name = 'ID_party'      on parlamentaria
      metadata_row_name = 'ID_candidate'  on presidencial
    """
    row = get_row_by_id_tema(df, metadata_row_name)
    if row is None:
        return {}

    entity_columns = get_entity_columns(df)
    out = {}
    for col in entity_columns:
        out[col] = normalize_id(row.get(col))
    return out


def build_presidential_party_column_map(pres_df):
    """
    Map party_id -> presidential candidate column name.

    This assumes presidential candidate column names contain the party in parentheses,
    e.g. 'Pedro Pérez (Partido X)', and that the normalized extracted party text
    matches the ID_party values used in parlamentaria.

    If that assumption fails, you can replace this with a direct metadata-based mapping.
    """
    pres_cols = get_entity_columns(pres_df)
    party_to_col = {}

    for c in pres_cols:
        s = clean_text(c)
        if not s:
            continue

        m = re.search(r"\(([^)]+)\)\s*$", s)
        if not m:
            continue

        party_text = clean_text(m.group(1))
        party_id = normalize_id(party_text)
        if party_id:
            party_to_col[party_id] = c

    return party_to_col


def build_question_lookup(df):
    """
    Build lookup from normalized ID_tema -> row index.
    This is now the canonical cross-sheet question matcher.
    """
    lookup = {}
    for i in range(df.shape[0]):
        id_tema = normalize_id(df.at[i, "ID_tema"])
        if id_tema is None:
            continue
        lookup[id_tema] = i
    return lookup


def generate_from_new_structure():
    # Load both sheets
    parl_raw_df = load_structure_sheet(NEW_STRUCTURE_FILE, "parlamentaria", number_of_topics)
    pres_raw_df = load_structure_sheet(NEW_STRUCTURE_FILE, "presidencial", number_of_topics)

    # Separate metadata rows from question rows
    parl_df = get_question_rows(parl_raw_df, number_of_topics)
    pres_df = get_question_rows(pres_raw_df, number_of_topics)

    # Identify party columns in parlamentaria
    party_columns = get_entity_columns(parl_raw_df)

    # Build metadata maps
    # parlamentaria: column -> party_id
    parl_party_ids = build_entity_id_map(parl_raw_df, "ID_party")

    # presidencial: column -> candidate_id
    pres_candidate_ids = build_entity_id_map(pres_raw_df, "ID_candidate")

    # Build presidential mapping: party_id -> candidate column
    pres_candidate_to_col = build_candidate_id_to_column_map(pres_raw_df)

    # Build a fast lookup from ID_tema -> row index in presidencial
    pres_index = build_question_lookup(pres_df)

    parties_info = {}
    for party_column in party_columns:
        party_id = parl_party_ids.get(party_column)
        parties_info[party_column] = {
            "header": party_column,
            "name": party_column,
            "party_id": party_id,
            "votes": {}
        }

    # Iterate parlamentaria question rows
    for row_index in range(parl_df.shape[0]):
        id_tema_value = clean_text(parl_df.at[row_index, "ID_tema"])
        topic_text = clean_text(parl_df.at[row_index, "Tema"])
        statement_text = clean_text(parl_df.at[row_index, "Statement"])

        if statement_text is None or id_tema_value is None:
            continue

        question_identifier = f"{topic_text}: {statement_text}" if topic_text else statement_text
        question_key, topic_key = build_question_key_from_id(id_tema_value)

        if question_key is None or topic_key is None:
            continue

        for party_column in party_columns:
            party_id = parl_party_ids.get(party_column)

            # 1) Try parlamentaria cell first
            cell_value = parl_df.at[row_index, party_column] if party_column in parl_df.columns else None
            vote_value, comment_value, source_value = parse_cell_combined(cell_value)

            comment_key = None


            if vote_value is not None or comment_value is not None or source_value is not None:
                # Data came from parlamentaria -> identify entity with ID_party
                if comment_value:
                    comment_key = build_comment_key("party", party_id, question_key)

            else:
                # 2) If missing, fallback to presidential candidate column for that party
                candidate_id_for_party = party_id_to_candidate_id(party_id)
                pres_col = pres_candidate_to_col.get(candidate_id_for_party)
                pres_row = pres_index.get(normalize_id(id_tema_value))

                print("-----")
                print("party_column:", party_column)
                print("party_id:", party_id)
                print("id_tema_value:", id_tema_value)
                print("parl cell:", cell_value)
                print("parl parsed:", vote_value, comment_value, source_value)

                candidate_id_for_party = party_id_to_candidate_id(party_id)
                pres_col = pres_candidate_to_col.get(candidate_id_for_party)
                pres_row = pres_index.get(normalize_id(id_tema_value))

                print("mapped pres_col:", pres_col)
                print("mapped pres_row:", pres_row)

                if pres_col in pres_raw_df.columns:
                    print("pres raw cell:", pres_raw_df.at[pres_row + 1 if pres_row is not None else 0, pres_col] if pres_row is not None else None)

                if pres_col and pres_row is not None and pres_col in pres_df.columns:
                    pres_cell_value = pres_df.at[pres_row, pres_col]
                    print("pres cell:", pres_cell_value)
                    print("pres parsed:", parse_cell_combined(pres_cell_value))

                if pres_col and pres_row is not None and pres_col in pres_df.columns:
                    pres_cell_value = pres_df.at[pres_row, pres_col]
                    vote_value, comment_value, source_value = parse_cell_combined(pres_cell_value)

                    candidate_id = pres_candidate_ids.get(pres_col)

                    if comment_value:
                        comment_value = (
                            f"Por falta de información, se tomó la posición del candidato: "
                            f"{comment_value}"
                        )
                        comment_key = build_comment_key("candidate", candidate_id, question_key)

            # 3) If still missing, SKIP this statement for this party
            if vote_value is None and comment_value is None and source_value is None:
                continue

            parties_info[party_column]["votes"][question_identifier] = {
                "id_tema": id_tema_value,
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