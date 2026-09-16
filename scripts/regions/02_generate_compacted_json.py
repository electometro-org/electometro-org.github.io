#!/usr/bin/env python3
"""
Convert the raw regional JSON (combined_votes_peru_regions_2026.json) into the
compact format, per region:
{
  "version": "x.x.x",
  "regions": {
    "Lima Metropolitana": {
      "quiz": {
        "PE1": { "id": "PE1", "topic": "Topic Name", "question": "Question text" },
        ...
      },
      "candidates": {
        "c1": {
          "id": "c1",
          "name": "Candidate Name",
          "party": {"name": "Party Name"},
          "votes": {
            "PE1": { "vote": 0.0, "comment": "...", "source": "..." },
            ...
          }
        },
        ...
      }
    },
    "Arequipa": {...},
    "Piura": {...}
  }
}
"""

import json
import os


def normalize_id(text):
    """Convert text to a normalized ID (lowercase, alphanumeric + underscore)."""
    import re

    if text is None:
        return None
    text = str(text).strip().lower()
    text = re.sub(r"[^\w\sáéíóúñü]", "", text, flags=re.UNICODE)
    text = re.sub(r"\s+", "_", text)
    text = text.strip("_")
    return text


def extract_topic_id(vote_data):
    """Extract topic ID from vote data."""
    if "id_tema" in vote_data:
        return vote_data["id_tema"]
    if "topic_key" in vote_data and vote_data["topic_key"]:
        return vote_data["topic_key"].replace("topics.", "")
    return None


def convert_to_new_format(input_data, entity_type="candidates"):
    """
    Convert one region's raw format into the compact format.

    Args:
        input_data: {"version": ..., "candidates": {...}}
        entity_type: "candidates"

    Returns:
        {"quiz": {...}, "candidates": {...}}
    """
    topics = {}
    entities = input_data.get(entity_type, {})

    for entity_name, entity_data in entities.items():
        votes = entity_data.get("votes", {})
        for question_key, vote_data in votes.items():
            topic_id = extract_topic_id(vote_data)
            if topic_id and topic_id not in topics:
                topic_name = vote_data.get("tema", "")
                question_text = vote_data.get("question", "")
                topics[topic_id] = {
                    "id": topic_id,
                    "topic": topic_name,
                    "question": question_text,
                }

    new_entities = {}

    for entity_name, entity_data in entities.items():
        votes = entity_data.get("votes", {})

        # The raw regions JSON already keys each candidate by its auto-generated
        # id (c1, c2, ... reset per region), so reuse that key directly instead
        # of re-deriving it — a candidate with zero votes has no comment_key to
        # extract an id from, which would otherwise collide with another entity.
        entity_id = entity_data.get("id", entity_name)

        new_votes = {}
        for question_key, vote_data in votes.items():
            topic_id = extract_topic_id(vote_data)
            if not topic_id:
                continue

            new_votes[topic_id] = {
                "vote": vote_data.get("vote"),
                "comment": vote_data.get("comment"),
                "source": vote_data.get("source"),
            }

        entity_entry = {
            "id": entity_id,
            "name": entity_data.get("name", entity_name),
            "votes": new_votes,
        }
        if entity_data.get("party"):
            entity_entry["party"] = {"name": entity_data.get("party")}
        new_entities[entity_id] = entity_entry

    return {"quiz": topics, entity_type: new_entities}


def main():
    OUTPUT_DIR_LATEST = os.path.join(os.path.dirname(__file__), "..", "..", "json", "regions", "latest")
    OUTPUT_DIR_HISTORY = os.path.join(os.path.dirname(__file__), "..", "..", "json", "regions", "history")

    os.makedirs(OUTPUT_DIR_LATEST, exist_ok=True)

    regions_input = os.path.join(OUTPUT_DIR_LATEST, "combined_votes_peru_regions_2026.json")

    if not os.path.exists(regions_input):
        print(f"Input file not found: {regions_input}")
        return

    with open(regions_input, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    version = raw_data.get("version", "0.0.0")
    raw_regions = raw_data.get("regions", {})

    compact_regions = {}
    for region_code, region_data in raw_regions.items():
        region_name = region_data.get("name", region_code)
        print(f"Converting region: {region_code} ({region_name})...")
        converted = convert_to_new_format(region_data, "candidates")
        converted = {"id": region_code, "name": region_name, **converted}
        compact_regions[region_code] = converted
        print(f"  -> {len(converted.get('quiz', {}))} topics, {len(converted.get('candidates', {}))} candidates")

    output = {"version": version, "regions": compact_regions}

    latest_path = os.path.join(OUTPUT_DIR_LATEST, "combined_votes_peru_regions_2026_compact.json")
    with open(latest_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"Wrote {latest_path}")

    if version and version != "0.0.0":
        version_underscored = version.replace(".", "_")
        version_folder = f"v{version_underscored}"
        history_version_dir = os.path.join(OUTPUT_DIR_HISTORY, version_folder)
        os.makedirs(history_version_dir, exist_ok=True)

        history_filename = f"combined_votes_peru_regions_2026_compact_{version_underscored}.json"
        history_path = os.path.join(history_version_dir, history_filename)
        with open(history_path, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        print(f"Wrote {history_path}")


if __name__ == "__main__":
    main()
