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

# Global counter and mapping for unique comments
comment_to_key_map = {}
comment_counter = 1

def text_to_key(text):
    """Convert Spanish text to a translation key in snake_case."""
    if text is None:
        return None
    text = str(text).strip().lower()
    text = re.sub(r'[^\w\sáéíóúñü]', '', text, flags=re.UNICODE)
    text = re.sub(r'\s+', '_', text)
    text = text.strip('_')
    return text

def get_comment_key(comment):
    """Get or create a translation key for a comment."""
    global comment_counter, comment_to_key_map

    if comment is None or comment == MISSING_COMMENT_DEFAULT:
        return None

    if comment in comment_to_key_map:
        return comment_to_key_map[comment]

    key = f"exp{comment_counter}"
    comment_to_key_map[comment] = key
    comment_counter += 1
    return key

def clean_text(s):
    if s is None:
        return None
    if pd.isna(s):
        return None
    s = str(s).strip()
    return s if s != "" else None

def map_vote_text_to_value(vote_text):
    """Map Spanish vote text or numeric strings to numeric values."""
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
    """Parse cell: "vote***comment***source" """
    raw = clean_text(cell_value)
    if raw is None:
        return MISSING_VOTE_DEFAULT, MISSING_COMMENT_DEFAULT, MISSING_SOURCE_DEFAULT

    parts = raw.split('***', 2)
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
        sheet_name="parlamentaria",
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

    party_columns = [col for col in data_frame.columns if col not in ("Tema", "Statement")]

    parties_info = {}
    for party_column in party_columns:
        parties_info[party_column] = {
            "header": party_column,
            "name": party_column,
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

        for party_column in party_columns:
            cell_value = data_frame.at[row_index, party_column] if party_column in data_frame.columns else None
            vote_value, comment_value, source_value = parse_cell_combined(cell_value)

            if vote_value is None and comment_value is None and source_value is None:
                vote_value = missing_vote_default
                comment_value = missing_comment_default
                source_value = missing_source_default

            comment_key = None
            if comment_value and comment_value != MISSING_COMMENT_DEFAULT:
                exp_key = get_comment_key(comment_value)
                comment_key = f"explanations.{exp_key}" if exp_key else None

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

    combined_output = {"parties": {}}
    for party_column, party_info in parties_info.items():
        combined_output["parties"][party_info["header"]] = {
            "name": party_info["name"],
            "votes": party_info["votes"]
        }

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, "combined_votes_peru_partidos_2026.json")
    with open(output_path, "w", encoding="utf-8") as file_handle:
        json.dump(combined_output, file_handle, ensure_ascii=False, indent=2)

    print(f"Wrote {output_path}")


if __name__ == "__main__":
    generate_from_new_structure()
