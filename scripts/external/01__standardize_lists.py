import json
from dataclasses import asdict
from itertools import chain
from collections import defaultdict

import requests
import pandas as pd

from epstein_photos.config import EXTRACTED_FACES_DIR, IMAGE_DIR, NETWORK_ROOT
from epstein_photos.utils import process_basename
from epstein_photos.utils import tommy_to_me

JMAIL_DATA_URL = 'https://jmail.world/api/photos'
TOMMY_DATA_URL = "https://tommycarstensen.com/epstein/data/gallery-db.js"

DATA_DIR = NETWORK_ROOT / "data"
TOMMY_JSON = DATA_DIR / "tommy_name_to_urls.json"

MAX_FOLDER_RANK = 1504

OUTPUT_DIR = DATA_DIR / "people_to_files"

def jmail_to_me(s):
    if "EFTA" not in s:
        return
    doc = s.split("-")[0]
    pg = int(s.split("-")[1].split(".")[0])
    return doc + f"-{pg:>05}.jpg"

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
    image_data = json.loads(r.text.split(";\nconst STAMP_RANGES")[0].split("DB_RAW=")[1])
    all_renamed_tommy_photo_ids = [f"EFTA{photo[1]:0>8}-{photo[2]-1:0>5}.jpg" for photo in image_data]

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

    df = pd.DataFrame([asdict(process_basename(folder.name)) for folder in folders])
    df = df[df["rank"] <= MAX_FOLDER_RANK].copy()

    with_name = df[df["name"].notna()].set_index("basename")["name"].apply(lambda s : s.replace("_", " ")).to_dict()
    without_name = df[((~df["to_ignore"]) & (df["name"].isna()))].set_index("basename")["person_id"].to_dict()
    folder_to_identifier = {**with_name, **without_name}

    my_person_to_files = defaultdict(list)

    for folder, identifier in folder_to_identifier.items():
        filenames = [file.name for file in (EXTRACTED_FACES_DIR / folder / "original").glob("*") if "_" not in file.name]
        my_person_to_files[identifier].extend(filenames)
    my_person_to_files = dict(my_person_to_files)

    OUTPUT_DIR.mkdir(exist_ok=True)

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
