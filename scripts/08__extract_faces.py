import os
import re
from shutil import copy2
from PIL import Image

# ---------------- CONFIG ----------------

from config import IMAGE_DIR
from faces_db import init_db

OUTPUT_DIR = "../../extracted_faces"
CELEBRITY_CONFIDENCE_THRESHOLD = 95.0

# --------------------------------------


def _slugify_name(name: str) -> str:
    """Turn a celebrity name into a safe directory suffix, e.g. 'Jeremy Hunt' -> 'Jeremy_Hunt'."""
    name = name.strip().replace(" ", "_")
    return re.sub(r"[^A-Za-z0-9_]+", "", name)


def extract_faces():
    conn = init_db()
    c = conn.cursor()

    # Fetch all data from faces (with person_id set)
    c.execute("""
        SELECT person_id, image_name, face_id, left, top, width, height,
               celebrity_name, celebrity_confidence
        FROM faces
        WHERE person_id IS NOT NULL
        ORDER BY person_id, image_name
    """)

    rows = c.fetchall()

    # Group by person_id, and track best celebrity (if any) per person
    people_data = {}
    best_celebrity = {}
    for row in rows:
        (
            person_id,
            image_name,
            face_id,
            left,
            top,
            width,
            height,
            celebrity_name,
            celebrity_confidence,
        ) = row
        if person_id not in people_data:
            people_data[person_id] = []
        people_data[person_id].append((image_name, face_id, left, top, width, height))

        if celebrity_name and celebrity_confidence is not None:
            prev = best_celebrity.get(person_id)
            if prev is None or celebrity_confidence > prev[1]:
                best_celebrity[person_id] = (celebrity_name, celebrity_confidence)

    # Sort by number of faces (descending) and assign ranks
    sorted_people = sorted(people_data.items(), key=lambda x: len(x[1]), reverse=True)

    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Process each person with rank
    TOP_FACES_PER_PERSON = 100
    for rank, (person_id, faces) in enumerate(sorted_people):
        ranked_dir_name = f"{rank:03d}__{person_id}"

        celeb = best_celebrity.get(person_id)
        if celeb is not None:
            celeb_name, celeb_conf = celeb
            if celeb_conf is not None and celeb_conf >= CELEBRITY_CONFIDENCE_THRESHOLD:
                suffix = _slugify_name(celeb_name)
                if suffix:
                    ranked_dir_name = f"{ranked_dir_name}__{suffix}"

        person_dir = os.path.join(OUTPUT_DIR, ranked_dir_name)
        faces_dir = os.path.join(person_dir, "faces")
        originals_dir = os.path.join(person_dir, "original")
        os.makedirs(faces_dir, exist_ok=True)
        os.makedirs(originals_dir, exist_ok=True)

        # Rank this person's faces by approximate face size (normalized bbox area × image pixels)
        scored_faces = []
        for image_name, face_id, left, top, width, height in faces:
            image_path = IMAGE_DIR / image_name
            if not image_path.is_file():
                raise FileNotFoundError(f"Image file missing: {image_path}")
            with Image.open(image_path) as img:
                img_width, img_height = img.size
            score = (width * height) * (img_width * img_height)
            scored_faces.append((score, image_name, face_id, left, top, width, height))

        scored_faces.sort(key=lambda x: x[0], reverse=True)
        faces_to_process = [
            (image_name, face_id, left, top, width, height)
            for _, image_name, face_id, left, top, width, height in scored_faces[:TOP_FACES_PER_PERSON]
        ]

        print(f"Processing {ranked_dir_name}: {len(faces_to_process)} faces (top {TOP_FACES_PER_PERSON} by size)")

        # Track which original images we've already copied for this person
        copied_originals = set()

        for image_name, face_id, left, top, width, height in faces_to_process:
            image_path = IMAGE_DIR / image_name
            if not image_path.is_file():
                raise FileNotFoundError(f"Image file missing: {image_path}")

            # Copy original image once per person
            if image_name not in copied_originals:
                copy2(image_path, os.path.join(originals_dir, image_name))
                copied_originals.add(image_name)

            # Load image for face crop
            with Image.open(image_path) as img:
                img_width, img_height = img.size
                x1 = int(left * img_width)
                y1 = int(top * img_height)
                x2 = int((left + width) * img_width)
                y2 = int((top + height) * img_height)
                face_crop = img.crop((x1, y1, x2, y2))

            # Save face crop
            # Use face_id as filename to ensure uniqueness
            output_filename = f"{os.path.splitext(image_name)[0]}_{face_id}.jpg"
            output_path = os.path.join(faces_dir, output_filename)
            face_crop.save(output_path, "JPEG")

    conn.close()
    print(f"\nDone! Extracted faces saved to {OUTPUT_DIR}/")

if __name__ == "__main__":
    extract_faces()
