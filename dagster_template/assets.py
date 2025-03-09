import dagster as dg
import requests
from typing import List, Dict, Any

starwars_api_asset = dg.AssetSpec("starwars_api")


def get_starwars_api_data(endpoint: str) -> List[Dict[str, Any]]:
    url = f"https://swapi.dev/api/{endpoint}/?format=json"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    return data["results"]


@dg.asset(deps=[starwars_api_asset], description="The people in the Star Wars API")
def starwars_people() -> List[Dict[str, Any]]:
    return get_starwars_api_data("people")


@dg.asset(deps=[starwars_api_asset], description="The planets in the Star Wars API")
def starwars_planets() -> List[Dict[str, Any]]:
    return get_starwars_api_data("planets")


@dg.asset(deps=[starwars_api_asset], description="The films in the Star Wars API")
def starwars_films() -> List[Dict[str, Any]]:
    return get_starwars_api_data("films")


@dg.asset(
    description="Summary statistics",
)
def starwars_statistics(
    starwars_people: List[Dict[str, Any]],
    starwars_planets: List[Dict[str, Any]],
    starwars_films: List[Dict[str, Any]],
) -> str:
    return "People: {0}, Planets: {1}, Films: {2}".format(
        len(starwars_people), len(starwars_planets), len(starwars_films)
    )
