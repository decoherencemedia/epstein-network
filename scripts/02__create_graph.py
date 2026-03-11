import sqlite3
from collections import defaultdict
from pathlib import Path
from functools import cache
import json
from shutil import copy

import pandas as pd
from itertools import combinations

import networkx as nx
from PIL import Image


SQLITE_DB = "faces.db"

PEOPLE_NAMES = {
    "person_7": "Jeffrey Epstein",
    "person_6": "Ghislaine Maxwell",
    "person_19": "Jean-Luc Brunel",
    "person_15": "Bill Clinton",
    "person_33": "David Mullen",
    "person_48": "Andrew Farkas",
    "person_4": "Doug Band",
    "person_37": "Larry Visoski",
    "person_18": "Juan Pablo Molyneaux",
    "person_14": "Walter Cronkite",
    "person_5": "Igor Zinoviev",
    "person_72": "Miles Alexander",
    "person_338": "Michael Jackson"
}

IMAGE_DIR = Path("../../all_images_parallel")

FILTERED_IMAGE_DIR = Path("../images")

@cache
def get_image_size(filename):
    image = Image.open(IMAGE_DIR / filename)
    return image.size[0] * image.size[1]

def highest_resolution(filenames: list[str]) -> str:
    return max(filenames, key=lambda f: get_image_size(f))

if __name__ == "__main__":
    con = sqlite3.connect(SQLITE_DB)
    df = pd.read_sql_query("SELECT * FROM people", con)
    df["person_id"] = df["person_id"].apply(lambda person_id : PEOPLE_NAMES.get(person_id, person_id))
    image_to_people = dict(df.groupby("image_name")["person_id"].agg(set))

    edges = defaultdict(int)
    edge_to_image_list = defaultdict(set)

    for image, people in image_to_people.items():
        for person_1, person_2 in combinations(people, 2):
            edges[tuple(sorted((person_1, person_2)))] += 1
            edge_to_image_list[tuple(sorted((person_1, person_2)))].add(image)

    edge_list = [(*k, v) for k, v in dict(edges).items()]

    G = nx.Graph()
    G.add_weighted_edges_from(edge_list)
    # G = nx.relabel_nodes(G, PEOPLE_NAMES)
    degree_root_2 = {k : v**0.5 for k, v in dict(G.degree()).items()}
    nx.set_node_attributes(G = G, values = degree_root_2, name = "degree_root_2")
    nx.write_graphml(G, "epstein_photo_people.graphml")

    node_images = dict()
    for node, filenames in dict(df.groupby("person_id")["image_name"].agg(set)).items():
        node_images[node] = highest_resolution(filenames)

    edge_images = dict()
    for edge, filenames in edge_to_image_list.items():
        edge_images["-".join(edge)] = highest_resolution(filenames)

    FILTERED_IMAGE_DIR.mkdir(exist_ok = True)

    for filename in node_images.values():
        copy(IMAGE_DIR / filename, FILTERED_IMAGE_DIR / filename)

    for filename in edge_images.values():
        copy(IMAGE_DIR / filename, FILTERED_IMAGE_DIR / filename)

    image_data = {
        "nodes": node_images,
        "edges": edge_images
    }

    with open("../image_data.json", "w") as f:
        json.dump(image_data, f)