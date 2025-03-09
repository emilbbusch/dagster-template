import dagster as dg
import requests
import json
from typing import List, Dict, Any

starwars_api_asset = dg.AssetSpec("starwars_api")


def get_starwars_api_data(endpoint: str) -> List[Dict[str, Any]]:
    url = f"https://swapi.dev/api/{endpoint}/?format=json"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    return data["results"]


def write_starwars_api_data_to_disk(data: List[Dict[str, Any]], filename: str) -> None:
    with open(f"data/starwars_{filename}.json", "w+") as file:
        file.write(json.dumps(data))


@dg.asset(deps=[starwars_api_asset], description="The people in the Star Wars API")
def starwars_people() -> None:
    data = get_starwars_api_data("people")
    return write_starwars_api_data_to_disk(data, "people")


@dg.asset(deps=[starwars_api_asset], description="The planets in the Star Wars API")
def starwars_planets() -> None:
    data = get_starwars_api_data("planets")
    return write_starwars_api_data_to_disk(data, "planets")


@dg.asset(deps=[starwars_api_asset], description="The films in the Star Wars API")
def starwars_films() -> None:
    data = get_starwars_api_data("films")
    return write_starwars_api_data_to_disk(data, "films")


@dg.asset(
    deps=[starwars_people, starwars_planets, starwars_films],
    description="Summary statistics",
)
def starwars_statistics() -> str:
    with open("data/starwars_people.json") as file:
        people = json.load(file)
    with open("data/starwars_planets.json") as file:
        planets = json.load(file)
    with open("data/starwars_films.json") as file:
        films = json.load(file)

    return "People: {0}, Planets: {1}, Films: {2}".format(
        len(people), len(planets), len(films)
    )
