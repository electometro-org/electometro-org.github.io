#!/usr/bin/env python3
"""
Convert the current JSON format to the new compact format:
{
  "version": "x.x.x",
  "quiz": {
    "t1": { "id": "t1", "topic": "Topic Name", "question": "Question text" },
    ...
  },
  "parties": {
    "p1": {
      "id": "p1",
      "name": "Party Name",
      "votes": {
        "t1": { "vote": 0.0, "comment": "...", "source": "..." },
        ...
      }
    },
    ...
  }
}
"""

import json
import os
import re
import sys


def normalize_id(text):
    """Convert text to a normalized ID (lowercase, alphanumeric + underscore)."""
    if text is None:
        return None
    text = str(text).strip().lower()
    text = re.sub(r"[^\w\sáéíóúñü]", "", text, flags=re.UNICODE)
    text = re.sub(r"\s+", "_", text)
    text = text.strip("_")
    return text


def extract_topic_id(vote_data):
    """Extract topic ID from vote data."""
    # Try id_tema first (e.g., "t1")
    if "id_tema" in vote_data:
        return vote_data["id_tema"]
    # Fallback to topic_key (e.g., "topics.t1")
    if "topic_key" in vote_data and vote_data["topic_key"]:
        return vote_data["topic_key"].replace("topics.", "")
    return None


def extract_entity_id(vote_data, entity_type="parties"):
    """Extract entity ID from comment_key if available."""
    # comment_key format: "explanations.parties.p1.t1" or "explanations.candidates.c1.t1"
    if "comment_key" in vote_data and vote_data["comment_key"]:
        parts = vote_data["comment_key"].split(".")
        if len(parts) >= 3:
            # parts[1] should match entity_type ("parties" or "candidates")
            if parts[1] == entity_type:
                return parts[2]
    return None


def convert_to_new_format(input_data, entity_type="parties"):
    """
    Convert the current format to the new compact format.

    Args:
        input_data: The current JSON data
        entity_type: "parties" or "candidates"

    Returns:
        The converted data in the new format
    """
    version = input_data.get("version", "0.0.0")

    # Build topics dict from the first entity's votes
    topics = {}
    entities = input_data.get(entity_type, {})

    # First pass: collect all topics
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
                    "question": question_text
                }

    # Build entities dict with new format
    new_entities = {}
    entity_id_counter = 1
    id_prefix = "p" if entity_type == "parties" else "c"

    for entity_name, entity_data in entities.items():
        votes = entity_data.get("votes", {})

        # Try to get entity ID from first vote's comment_key
        entity_id = None
        for vote_data in votes.values():
            entity_id = extract_entity_id(vote_data, entity_type)
            if entity_id:
                break

        # Fallback: generate an ID from the name
        if not entity_id:
            entity_id = f"{id_prefix}{entity_id_counter}"
            entity_id_counter += 1

        # Build votes dict keyed by topic ID
        new_votes = {}
        for question_key, vote_data in votes.items():
            topic_id = extract_topic_id(vote_data)
            if not topic_id:
                continue

            new_votes[topic_id] = {
                "vote": vote_data.get("vote"),
                "comment": vote_data.get("comment"),
                "source": vote_data.get("source")
            }

        entity_entry = {
            "id": entity_id,
            "name": entity_data.get("name", entity_name),
            "votes": new_votes
        }
        # Store party info for candidates (will be enriched later with party ID)
        if entity_type == "candidates" and entity_data.get("party"):
            entity_entry["party"] = {"name": entity_data.get("party")}
        new_entities[entity_id] = entity_entry

    # Build output
    output = {
        "version": version,
        "quiz": topics,
        entity_type: new_entities
    }

    return output


def main():
    # Same directory as build_data scripts
    OUTPUT_DIR_LATEST = os.path.join(os.path.dirname(__file__), "..", "json", "latest")

    # Ensure directory exists
    os.makedirs(OUTPUT_DIR_LATEST, exist_ok=True)

    # Input files
    parties_input = os.path.join(OUTPUT_DIR_LATEST, "combined_votes_peru_partidos_2026.json")
    candidates_input = os.path.join(OUTPUT_DIR_LATEST, "combined_votes_peru_pres_2026.json")

    # Convert both files
    parties_data = None
    candidates_data = None

    if os.path.exists(parties_input):
        print("Converting parties...")
        with open(parties_input, "r", encoding="utf-8") as f:
            parties_data = convert_to_new_format(json.load(f), "parties")
        print(f"  -> {len(parties_data.get('quiz', {}))} topics, {len(parties_data.get('parties', {}))} parties")

    if os.path.exists(candidates_input):
        print("Converting candidates...")
        with open(candidates_input, "r", encoding="utf-8") as f:
            candidates_data = convert_to_new_format(json.load(f), "candidates")
        print(f"  -> {len(candidates_data.get('quiz', {}))} topics, {len(candidates_data.get('candidates', {}))} candidates")

    # Cross-reference parties and candidates
    if parties_data and candidates_data:
        print("Cross-referencing parties and candidates...")

        # Build party name -> party info mapping
        party_name_to_info = {}
        for party_id, party_info in parties_data.get("parties", {}).items():
            party_name = party_info.get("name", "")
            party_name_to_info[party_name] = {"id": party_id, "name": party_name}

        # Build party name -> candidate info mapping
        party_to_candidate = {}
        for candidate_id, candidate_info in candidates_data.get("candidates", {}).items():
            party_obj = candidate_info.get("party", {})
            party_name = party_obj.get("name", "") if isinstance(party_obj, dict) else ""
            if party_name:
                party_to_candidate[party_name] = {"id": candidate_id, "name": candidate_info.get("name", "")}

        # Enrich candidates with party ID
        for candidate_id, candidate_info in candidates_data.get("candidates", {}).items():
            party_obj = candidate_info.get("party", {})
            if isinstance(party_obj, dict):
                party_name = party_obj.get("name", "")
                if party_name in party_name_to_info:
                    candidate_info["party"] = party_name_to_info[party_name]

        # Enrich parties with candidate info
        for party_id, party_info in parties_data.get("parties", {}).items():
            party_name = party_info.get("name", "")
            if party_name in party_to_candidate:
                party_info["candidate"] = party_to_candidate[party_name]

    # Write output files
    if parties_data:
        output_path = os.path.join(OUTPUT_DIR_LATEST, "combined_votes_peru_partidos_2026_compact.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(parties_data, f, ensure_ascii=False, indent=2)
        print(f"  -> Wrote {output_path}")

    if candidates_data:
        output_path = os.path.join(OUTPUT_DIR_LATEST, "combined_votes_peru_pres_2026_compact.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(candidates_data, f, ensure_ascii=False, indent=2)
        print(f"  -> Wrote {output_path}")


if __name__ == "__main__":
    main()