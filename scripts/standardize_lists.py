import json
from pathlib import Path
from itertools import chain
import sqlite3
from collections import defaultdict, Counter

import requests
import pandas as pd

JMAIL_DATA_URL = 'https://jmail.world/api/photos'
TOMMY_DATA_URL = "https://tommycarstensen.com/epstein/data/gallery-db.js"
TOMMY_JSON = "../jupyter/people_photo_links.json"

IMAGE_DIR = Path("../../../all_images/")
EXTRACTED_FACES_DIR = Path("../../extracted_faces/")

MAX_FOLDER_RANK = 1504

OUTPUT_DIR = Path("person_to_files/")

def jmail_to_me(s):
    if "EFTA" not in s:
        return
    doc = s.split("-")[0]
    pg = int(s.split("-")[1].split(".")[0])
    return doc + f"-{pg:>04}.jpg"

def tommy_to_me(s):
    filename = s.strip("/").split("/")[-1]
    doc = filename.split("_")[0]
    pg = int(filename.split("_p")[1].split("_")[0])
    return doc + f"-{pg-1:>04}.jpg"

def process_basename(basename):
    parts = basename.split("__")
    to_ignore = parts[-1] == "IGNORE"

    match parts:
        # Any folder explicitly marked IGNORE should not contribute a name.
        case [rank, person_id, "IGNORE"]:
            name = None
        case [rank, person_id, name, "IGNORE"]:
            name = None
        case [rank, person_id]:
            name = None
        case [rank, person_id, name]:
            name = name
        case _:
            raise ValueError(f"folder {basename} not structured as expected")

    return {"basename": basename, "rank": int(rank), "person_id": person_id, "name": name, "to_ignore": to_ignore}

def sort_dict(data):
    return  dict(
        sorted(data.items(), key=lambda item: len(item[1]), reverse=True)
    )

if __name__ == "__main__":

    # JMAIL
    #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    params = {
        'newOnly': 'false',
    }
    response = requests.get(JMAIL_DATA_URL, params=params)
    all_jmail_photos = response.json()["photos"]
    photo_to_names = {d["id"] : d["person_ids"] for d in all_jmail_photos if d["person_ids"]}
    all_renamed_jmail_photo_ids = list(filter(None, (jmail_to_me(photo["id"]) for photo in all_jmail_photos)))

    # Invert dict of lists of people to dict of lists of files
    jmail_person_to_files = {}
    for k, v in photo_to_names.items():
        for x in v:
            if (jmail := jmail_to_me(k)):
                jmail_person_to_files.setdefault(x.replace("-", " ").title(), []).append(jmail)

    # Tommy
    #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    r = requests.get(TOMMY_DATA_URL)
    image_data = json.loads(r.text.split(";")[0].split("DB=")[1])
    all_renamed_tommy_photo_ids = [f"EFTA{photo['id']:0>8}-{photo['pg']-1:0>4}.jpg" for photo in image_data]

    with open(TOMMY_JSON, "r") as f:
        tommy_person_to_urls = json.load(f)
    all_renamed_tommy_photo_ids_with_faces = [tommy_to_me(url) for url in sorted(set(chain.from_iterable(tommy_person_to_urls.values())))]

    tommy_convert_name = lambda s : s.removeprefix("people/").removesuffix(".html").replace("-", " ").title()
    tommy_person_to_files = {}
    for person, urls in tommy_person_to_urls.items():
        tommy_person_to_files[tommy_convert_name(person)] = [tommy_to_me(url) for url in urls]

    # Me
    #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


    folders = sorted(list(EXTRACTED_FACES_DIR.glob("*")))

    df = pd.DataFrame([process_basename(folder.name) for folder in folders])
    df = df[df["rank"] <= MAX_FOLDER_RANK].copy()

    with_name = df[df["name"].notna()].set_index("basename")["name"].apply(lambda s : s.replace("_", " ")).to_dict()
    without_name = df[((~df["to_ignore"]) & (df["name"].isna()))].set_index("basename")["person_id"].to_dict()
    folder_to_identifier = {**with_name, **without_name}

    my_person_to_files = defaultdict(list)

    for folder, identifier in folder_to_identifier.items():
        filenames = [file.name for file in (EXTRACTED_FACES_DIR / folder / "original").glob("*") if "_" not in file.name]
        my_person_to_files[identifier].extend(filenames)
    my_person_to_files = dict(my_person_to_files)

    with open(OUTPUT_DIR / "me.json", "w") as f:
        json.dump(sort_dict(my_person_to_files), f, indent=4)

    with open(OUTPUT_DIR / "jmail.json", "w") as f:
        json.dump(sort_dict(jmail_person_to_files), f, indent=4)

    with open(OUTPUT_DIR / "tommy.json", "w") as f:
        json.dump(sort_dict(tommy_person_to_files), f, indent=4)

    # Verify I have most photos
    #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    all_my_photos = [file.name for file in IMAGE_DIR.glob("*")]

    jmail_not_me = (set(all_renamed_jmail_photo_ids) - set(all_my_photos))
    tommy_not_me = (set(all_renamed_tommy_photo_ids) - set(all_my_photos))
    tommy_faces_not_me = (set(all_renamed_tommy_photo_ids_with_faces) - set(all_my_photos))

    print(len(jmail_not_me))
    print(len(tommy_not_me))
    print(len(tommy_faces_not_me))
