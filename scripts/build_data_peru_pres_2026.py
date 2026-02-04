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
    if s is None:
        return None
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

def parse_cell_combined(cell_value):
    """Parse 'vote+++comment+++source' format"""
    raw = clean_text(cell_value)
    if raw is None:
        return MISSING_VOTE_DEFAULT, MISSING_COMMENT_DEFAULT, MISSING_SOURCE_DEFAULT

    parts = raw.split('+++', 2)
    vote_part = clean_text(parts[0]) if len(parts) >= 1 else None
    comment_part = clean_text(parts[1]) if len(parts) >= 2 else None
    source_part = clean_text(parts[2]) if len(parts) >= 3 else None

    vote_mapped = map_vote_text_to_value(vote_part)
    return vote_mapped, comment_part, source_part

def generate_from_new_structure():
    missing_vote_default = 0.5
    missing_comment_default = "No se encontró..."
    missing_source_default = None

    raw_dataframe = pd.read_excel(
        NEW_STRUCTURE_FILE,
        sheet_name="presidencial",
        dtype=str,
        header=None
    )

    raw_dataframe = raw_dataframe.head(number_of_topics + 1)
    if raw_dataframe.shape[0] < 1:
        raise ValueError("Input sheet appears empty or is missing the header row.")

    raw_dataframe.columns = raw_dataframe.iloc[0]
    data_frame = raw_dataframe.drop(index=0).reset_index(drop=True)

    if "Statement" not in data_frame.columns or "Tema" not in data_frame.columns:
        raise ValueError("Expected 'Tema' and 'Statement' columns in the sheet.")

    candidate_columns = [col for col in data_frame.columns if col not in ("Tema", "Statement")]

    candidates_info = {}
    for candidate_column in candidate_columns:
        candidate_name, candidate_party = parse_candidate_header(candidate_column)
        candidates_info[candidate_column] = {
            "header": candidate_column,
            "name": candidate_name,
            "party": candidate_party,
            "votes": {}
        }

    for row_index in range(data_frame.shape[0]):
        topic_raw_value = data_frame.at[row_index, "Tema"] if "Tema" in data_frame.columns else None
        statement_raw_value = data_frame.at[row_index, "Statement"] if "Statement" in data_frame.columns else None

        topic_text = clean_text(topic_raw_value)
        statement_text = clean_text(statement_raw_value)

        if statement_text is None:
            continue

        question_identifier = f"{topic_text}: {statement_text}" if topic_text else statement_text
        question_key = f"questions.{text_to_key(statement_text)}" if statement_text else None
        topic_key = f"topics.{text_to_key(topic_text)}" if topic_text else None

        for candidate_column in candidate_columns:
            cell_value = data_frame.at[row_index, candidate_column] if candidate_column in data_frame.columns else None
            vote_value, comment_value, source_value = parse_cell_combined(cell_value)

            if vote_value is None and comment_value is None and source_value is None:
                vote_value = missing_vote_default
                comment_value = missing_comment_default
                source_value = missing_source_default

            comment_key = None
            if comment_value and comment_value != MISSING_COMMENT_DEFAULT:
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
