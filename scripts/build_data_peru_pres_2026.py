import pandas as pd
import os
import json
import re

NEW_STRUCTURE_FILE = os.getenv('PERU_FILE')
OUTPUT_DIR = "json/"

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

    # pd.isna is safe for scalars
    if pd.isna(s):
        return None

    s = str(s).strip()
    return s if s != "" else None

def parse_candidate_header(header_str):
    """'Name (Party)' -> (name, party)"""
    if header_str is None:
        return None, None
    header_str = str(header_str).strip()
    m = re.match(r'^(.*?)\s*\((.*?)\)\s*$', header_str)
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


def build_party_column_map(parl_df):
    """
    Map normalized party key -> actual party column name in parlamentaria.
    parlamentaria columns are party names (no candidate name).
    """
    party_cols = [c for c in parl_df.columns if c not in ("Tema", "Statement")]
    return {text_to_key(c): c for c in party_cols}

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
    pres_df = load_structure_sheet(NEW_STRUCTURE_FILE, "presidencial", number_of_topics)
    parl_df = load_structure_sheet(NEW_STRUCTURE_FILE, "parlamentaria", number_of_topics)

    # Candidate columns in presidencial
    candidate_columns = [col for col in pres_df.columns if col not in ("Tema", "Statement")]

    # Build candidate metadata (name + party from header)
    candidates_info = {}
    for candidate_column in candidate_columns:
        candidate_name, candidate_party = parse_candidate_header(candidate_column)
        candidates_info[candidate_column] = {
            "header": candidate_column,
            "name": candidate_name,
            "party": candidate_party,
            "votes": {}
        }

    # Map party -> parlamentaria column
    party_to_parl_col = build_party_column_map(parl_df)

    # Row index lookups by (tema, statement) for both sheets (robust if row orders differ)
    pres_index = {}
    for i in range(pres_df.shape[0]):
        t = clean_text(pres_df.at[i, "Tema"])
        s = clean_text(pres_df.at[i, "Statement"])
        if s is None:
            continue
        pres_index[(t, s)] = i

    parl_index = {}
    for i in range(parl_df.shape[0]):
        t = clean_text(parl_df.at[i, "Tema"])
        s = clean_text(parl_df.at[i, "Statement"])
        if s is None:
            continue
        parl_index[(t, s)] = i

    # Iterate over presidential statements using pres_df as the driver
    for (topic_text, statement_text), pres_row in pres_index.items():
        if statement_text is None:
            continue

        question_identifier = f"{topic_text}: {statement_text}" if topic_text else statement_text
        question_key = f"questions.{text_to_key(statement_text)}"
        topic_key = f"topics.{text_to_key(topic_text)}" if topic_text else None

        for candidate_column in candidate_columns:
            # 1) Try candidate cell in presidencial
            cell_value = pres_df.at[pres_row, candidate_column]
            vote_value, comment_value, source_value = parse_cell_combined(cell_value)

            # 2) If missing, fallback to the candidate's party cell in parlamentaria
            if vote_value is None and comment_value is None and source_value is None:
                candidate_party = candidates_info[candidate_column]["party"]
                party_key = text_to_key(candidate_party) if candidate_party else None
                parl_col = party_to_parl_col.get(party_key) if party_key else None
                parl_row = parl_index.get((topic_text, statement_text))

                if parl_col and parl_row is not None and parl_col in parl_df.columns:
                    party_cell_value = parl_df.at[parl_row, parl_col]
                    vote_value, comment_value, source_value = parse_cell_combined(party_cell_value)

            # 3) If still missing, SKIP this statement for this candidate
            if vote_value is None and comment_value is None and source_value is None:
                continue

            # Candidate comment key (even if the content came from party fallback)
            comment_key = None
            if comment_value:
                candidate_name = candidates_info[candidate_column]["name"]
                comment_key = build_comment_key("candidate", candidate_name, topic_key)

            candidates_info[candidate_column]["votes"][question_identifier] = {
                "tema": topic_text,
                "question": statement_text,
                "question_key": question_key,
                "topic_key": topic_key,
                "vote": vote_value,
                "comment": comment_value,
                "comment_key": comment_key,
                "source": source_value
            }

    combined_output = {"candidates": {}}
    for candidate_column, candidate_info in candidates_info.items():
        combined_output["candidates"][candidate_info["header"]] = {
            "name": candidate_info["name"],
            "party": candidate_info["party"],
            "votes": candidate_info["votes"]
        }

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, "combined_votes_peru_pres_2026.json")
    with open(output_path, "w", encoding="utf-8") as file_handle:
        json.dump(combined_output, file_handle, ensure_ascii=False, indent=2)

    print(f"Wrote {output_path}")


if __name__ == "__main__":
    generate_from_new_structure()
