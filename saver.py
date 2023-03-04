from math import ceil
import aiohttp
import asyncio
import os.path

import pyproj
from PIL import Image

GOVMAP_CDN = "https://cdn.govmap.gov.il"
MAP_NAME_25K = "022023MAP25KTO"
MAP_LODS = [
    {
        "level": 0,
        "resolution": 13.2291931250529,
        "scale": 50000,
        "startTileRow": 0,
        "startTileCol": 0,
        "endTileRow": 8017,
        "endTileCol": 7512
    },
    {
        "level": 1,
        "resolution": 6.61459656252646,
        "scale": 25000,
        "startTileRow": 0,
        "startTileCol": 0,
        "endTileRow": 16035,
        "endTileCol": 15024
    },
    {
        "level": 2,
        "resolution": 2.64583862501058,
        "scale": 10000,
        "startTileRow": 0,
        "startTileCol": 0,
        "endTileRow": 40089,
        "endTileCol": 37560
    },
    {
        "level": 3,
        "resolution": 1.32291931250529,
        "scale": 5000,
        "startTileRow": 0,
        "startTileCol": 0,
        "endTileRow": 80179,
        "endTileCol": 75121
    },
    {
        "level": 4,
        "resolution": 0.66145965625264,
        "scale": 2500,
        "startTileRow": 0,
        "startTileCol": 0,
        "endTileRow": 160359,
        "endTileCol": 150243
    }
]
MAP_XMIN = -5403700
MAP_YMAX = 7116700
TILE_SIZE = 256

CONCURRENT_REQUESTS = 5
limit = asyncio.Semaphore(CONCURRENT_REQUESTS)

EPSG_PREFIX = "EPSG:"
GOVMAP_WKID = f"{EPSG_PREFIX}2039"
WGS84_WKID = f"{EPSG_PREFIX}4326"
TRANSFORMER = pyproj.Transformer.from_crs(crs_from=WGS84_WKID, crs_to=GOVMAP_WKID)

REQUEST_HEADERS = {
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"
}


def transform(longitude: float, latitude: float):
    return TRANSFORMER.transform(latitude, longitude)


async def download_image(url, filename):
    if os.path.exists(filename):
        print(f"(skip) {filename} already downloaded")
        return

    dirname = os.path.dirname(filename)
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    async with limit:
        async with aiohttp.ClientSession(headers=REQUEST_HEADERS) as session:
            async with session.get(url, ssl=False) as resp:
                if resp.status != 200:
                    raise ValueError(f"Failed to download image from {url}")
                with open(filename, "wb") as f:
                    while True:
                        chunk = await resp.content.read(1024)
                        if not chunk:
                            break
                        f.write(chunk)
    print(f"(V) Download {filename}")


def merge_images(arr):
    # Get the width and height of the images
    width, height = Image.open(arr[0][0]).size

    # Create a new image with the same size
    result = Image.new('RGB', (width * len(arr), height * len(arr[0])))

    # Iterate through the array and paste each image into the result
    for i in range(len(arr)):
        for j in range(len(arr[i])):
            img = Image.open(arr[i][j])
            result.paste(img, (i * width, j * height))
    return result


async def main(x: float, y: float, width: int, height: int, name: str = "result_map_2.png"):
    zoom = 2
    resolution = MAP_LODS[zoom]['resolution']
    tile_resolution = TILE_SIZE * resolution

    center_row = int((MAP_YMAX - y) / tile_resolution)
    center_column = int((x - MAP_XMIN) / tile_resolution)

    tasks = []
    table = []
    for column_offset in range(-ceil(width / tile_resolution), ceil(width / tile_resolution)):
        column_arr = []
        for row_offset in range(-ceil(height / tile_resolution), ceil(height / tile_resolution)):
            row = center_row + row_offset
            column = center_column + column_offset
            govmap_filepath = f"L{zoom:02}/R{row:08x}/C{column:08x}.png"
            filename = f"tiles_cache/{govmap_filepath}"
            column_arr.append(filename)
            request_url = f"{GOVMAP_CDN}/{MAP_NAME_25K}/{govmap_filepath}"

            tasks.append(asyncio.create_task(
                download_image(request_url, filename)
            ))
        table.append(column_arr)
    await asyncio.gather(*tasks)

    merged_image = merge_images(table)
    merged_image.save(name)


if __name__ == '__main__':
    # address = "דרךדרך ירושלים 13, רחובות ירושלים 13, רחובות"
    # lon, lat = name_to_coordinates(address)

    lat, lon = 30.82717509247067, 34.825035482754366
    x, y = transform(longitude=lon, latitude=lat)

    width, height = 3_000, 2_000

    asyncio.run(main(x, y, width, height))
