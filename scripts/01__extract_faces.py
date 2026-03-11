import os
import sqlite3
from PIL import Image

# ---------------- CONFIG ----------------

DB_PATH = "faces.db"
IMAGE_DIR = "../../all_images_parallel"
OUTPUT_DIR = "extracted_faces"

# --------------------------------------

def extract_faces():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Fetch all data from people table
    c.execute("""
        SELECT person_id, image_name, face_id, left, top, width, height
        FROM people
        ORDER BY person_id, image_name
    """)

    rows = c.fetchall()

    # Group by person_id
    people_data = {}
    for row in rows:
        person_id, image_name, face_id, left, top, width, height = row
        if person_id not in people_data:
            people_data[person_id] = []
        people_data[person_id].append((image_name, face_id, left, top, width, height))

    # Sort by number of faces (descending) and assign ranks
    sorted_people = sorted(people_data.items(), key=lambda x: len(x[1]), reverse=True)

    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Process each person with rank
    for rank, (person_id, faces) in enumerate(sorted_people):
        ranked_dir_name = f"{rank:03d}__{person_id}"
        person_dir = os.path.join(OUTPUT_DIR, ranked_dir_name)
        os.makedirs(person_dir, exist_ok=True)

        print(f"Processing {ranked_dir_name}: {len(faces)} faces")

        for image_name, face_id, left, top, width, height in faces:
            image_path = os.path.join(IMAGE_DIR, image_name)

            if not os.path.exists(image_path):
                print(f"  Warning: Image not found: {image_path}")
                continue

            # Load image
            img = Image.open(image_path)
            img_width, img_height = img.size

            # Convert normalized coordinates to pixel coordinates
            # Rekognition bounding box: (left, top) is top-left corner, width/height are relative
            x1 = int(left * img_width)
            y1 = int(top * img_height)
            x2 = int((left + width) * img_width)
            y2 = int((top + height) * img_height)

            # Crop face
            face_crop = img.crop((x1, y1, x2, y2))

            # Save face crop
            # Use face_id as filename to ensure uniqueness
            output_filename = f"{os.path.splitext(image_name)[0]}_{face_id}.jpg"
            output_path = os.path.join(person_dir, output_filename)
            face_crop.save(output_path, "JPEG")

    conn.close()
    print(f"\nDone! Extracted faces saved to {OUTPUT_DIR}/")

if __name__ == "__main__":
    extract_faces()
