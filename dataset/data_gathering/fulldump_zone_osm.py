import json
import time
import requests
from typing import Dict, Any, List, Optional


# -----------------------------
# User parameters
# -----------------------------

CITY_NAME = "Andria"
COUNTRY_NAME = "Italy"

INCLUDE_ALL_OSM_OBJECTS = True
# Set to True if you really want the third query:
# all nodes, ways, and relations in the city.
# Warning: this can be huge and may timeout or overload Overpass.

OUTPUT_FILE = f"{CITY_NAME.lower().replace(' ', '_')}_overpass_export.geojson"

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
TIMEOUT_SECONDS = 180


# -----------------------------
# Overpass query
# -----------------------------


def build_overpass_query(
    city_name: str,
    country_name: Optional[str] = None,
    include_all_osm_objects: bool = False,
    timeout: int = 180,
) -> str:
    """
    Builds a fused Overpass query for:
    - street names and house numbers
    - waste-related POIs
    - optionally all OSM objects inside the city boundary
    """

    country_filter = f'["is_in:country"="{country_name}"]' if country_name else ""

    all_osm_block = ""
    if include_all_osm_objects:
        all_osm_block = """
  // All OSM objects inside the city boundary
  nwr(area.searchArea);
"""

    query = f"""
[out:json][timeout:{timeout}];

// Find administrative area by city name
area
  ["name"="{city_name}"]
  ["boundary"="administrative"]
  {country_filter}
  ->.searchArea;

(
  // Addresses: street names and house numbers
  nwr["addr:housenumber"](area.searchArea);

  // Small pedestrian trash cans / litter bins
  nwr["amenity"="waste_basket"](area.searchArea);

  // Medium/large street or household waste containers
  nwr["amenity"="waste_disposal"](area.searchArea);

  // Recycling containers and recycling centres
  nwr["amenity"="recycling"](area.searchArea);

  // Dog waste bins / dog poop bag disposal
  nwr["amenity"="waste_basket"]["waste"="dog_excrement"](area.searchArea);
  nwr["waste"="dog_excrement"](area.searchArea);
  nwr["amenity"="dog_waste_bin"](area.searchArea);

  // Cigarette / ash-only bins
  nwr["amenity"="waste_basket"]["waste"="cigarettes"](area.searchArea);
  nwr["waste"="cigarettes"](area.searchArea);

  // Bag dispensers for dog waste
  nwr["amenity"="vending_machine"]["vending"="excrement_bags"](area.searchArea);
  nwr["vending"="excrement_bags"](area.searchArea);

  // Facilities related to waste collection / processing
  nwr["amenity"="waste_transfer_station"](area.searchArea);
  nwr["amenity"="waste_dump_site"](area.searchArea);
  nwr["landuse"="landfill"](area.searchArea);

  // Sanitary / caravan waste dump points
  nwr["amenity"="sanitary_dump_station"](area.searchArea);

  // Less common, but relevant waste-related tags
  nwr["amenity"="animal_effluent_disposal"](area.searchArea);
  nwr["bin"="yes"](area.searchArea);

{all_osm_block}
);

// Return tags and usable geometry.
// `center` gives a point for ways/relations.
// `geom` gives full geometry where available.
out tags center geom qt;
"""
    return query.strip()


# -----------------------------
# Overpass request
# -----------------------------


def run_overpass_query(query: str) -> Dict[str, Any]:
    response = requests.post(
        OVERPASS_URL,
        data={"data": query},
        timeout=TIMEOUT_SECONDS + 30,
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"Overpass request failed with HTTP {response.status_code}:\n"
            f"{response.text[:1000]}"
        )

    return response.json()


# -----------------------------
# Classification helpers
# -----------------------------


def classify_feature(tags: Dict[str, str]) -> List[str]:
    """
    Adds broad categories to each GeoJSON feature.
    """

    categories = []

    if "addr:housenumber" in tags:
        categories.append("address")

    if (
        tags.get("amenity")
        in {
            "waste_basket",
            "waste_disposal",
            "recycling",
            "waste_transfer_station",
            "waste_dump_site",
            "sanitary_dump_station",
            "animal_effluent_disposal",
            "dog_waste_bin",
        }
        or tags.get("landuse") == "landfill"
        or tags.get("waste") in {"dog_excrement", "cigarettes"}
        or tags.get("bin") == "yes"
        or tags.get("vending") == "excrement_bags"
    ):
        categories.append("waste")

    if not categories:
        categories.append("other_osm")

    return categories


# -----------------------------
# GeoJSON conversion
# -----------------------------


def is_closed_way(coords: List[List[float]]) -> bool:
    return len(coords) >= 4 and coords[0] == coords[-1]


def element_to_geojson_feature(element: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    element_type = element.get("type")
    element_id = element.get("id")
    tags = element.get("tags", {})

    geometry = None

    if element_type == "node":
        if "lat" in element and "lon" in element:
            geometry = {
                "type": "Point",
                "coordinates": [element["lon"], element["lat"]],
            }

    elif element_type == "way":
        if "geometry" in element:
            coords = [
                [point["lon"], point["lat"]]
                for point in element["geometry"]
                if "lon" in point and "lat" in point
            ]

            if len(coords) >= 2:
                if is_closed_way(coords):
                    geometry = {
                        "type": "Polygon",
                        "coordinates": [coords],
                    }
                else:
                    geometry = {
                        "type": "LineString",
                        "coordinates": coords,
                    }

        elif "center" in element:
            geometry = {
                "type": "Point",
                "coordinates": [
                    element["center"]["lon"],
                    element["center"]["lat"],
                ],
            }

    elif element_type == "relation":
        # Relations can be complex multipolygons/routes.
        # For simplicity, this script exports their center point if available.
        if "center" in element:
            geometry = {
                "type": "Point",
                "coordinates": [
                    element["center"]["lon"],
                    element["center"]["lat"],
                ],
            }

    if geometry is None:
        return None

    properties = {
        "osm_type": element_type,
        "osm_id": element_id,
        "feature_categories": classify_feature(tags),
        **tags,
    }

    return {
        "type": "Feature",
        "geometry": geometry,
        "properties": properties,
    }


def overpass_to_geojson(overpass_data: Dict[str, Any]) -> Dict[str, Any]:
    features = []

    for element in overpass_data.get("elements", []):
        feature = element_to_geojson_feature(element)
        if feature is not None:
            features.append(feature)

    return {
        "type": "FeatureCollection",
        "features": features,
    }


# -----------------------------
# Main
# -----------------------------


def main() -> None:
    query = build_overpass_query(
        city_name=CITY_NAME,
        country_name=COUNTRY_NAME,
        include_all_osm_objects=INCLUDE_ALL_OSM_OBJECTS,
        timeout=TIMEOUT_SECONDS,
    )

    print("Running Overpass query...")
    print(f"City: {CITY_NAME}")
    print(f"Include all OSM objects: {INCLUDE_ALL_OSM_OBJECTS}")

    start = time.time()
    overpass_data = run_overpass_query(query)
    elapsed = time.time() - start

    print(
        f"Downloaded {len(overpass_data.get('elements', []))} OSM elements in {elapsed:.1f}s"
    )

    geojson = overpass_to_geojson(overpass_data)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(geojson['features'])} GeoJSON features to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
