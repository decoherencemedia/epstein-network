from pathlib import Path
import json

from bs4 import BeautifulSoup
import requests


TOMMY_BASE_URL = "https://tommycarstensen.com/epstein/"

OUTPUT_DIR = Path("data")
TOMMY_JSON = OUTPUT_DIR / "tommy_name_to_urls.json"


def tommy_to_me(s):
    filename = s.strip("/").split("/")[-1]
    doc = filename.split("_")[0]
    pg = int(filename.split("_p")[1].split("_")[0])
    return doc + f"-{pg-1:>05}.jpg"


if __name__ == "__main__":

    r = requests.get(TOMMY_BASE_URL + "people.html?photos=yes")
    soup = BeautifulSoup(r.content, features = "lxml")
    people_grid = soup.find("div", class_ = "people-grid")
    person_links = [a["href"] for a in people_grid.find_all("a", href = True) if int(a["data-photocount"]) > 0]

    person_to_photos = {}
    for i, person_link in enumerate(person_links):
        print(f"fetching {person_link}  |  {i}/{len(person_links)}")
        r = requests.get(TOMMY_BASE_URL + person_link)
        soup = BeautifulSoup(r.content, features = "lxml")

        photo_gallery = soup.find("div", class_ = "photo-gallery")
        photo_links = [img["src"].replace("../", "").replace("thumbnails", "full_res").replace(".jpg", ".png") for img in photo_gallery.find_all("img")]
        person_to_photos[person_link] = photo_links
        print(f"{len(photo_links)} photos for link {person_link}")

    OUTPUT_DIR.mkdir(exist_ok=True)

    with open(TOMMY_JSON, "w") as f:
        json.dump(obj=person_to_photos, fp=f)