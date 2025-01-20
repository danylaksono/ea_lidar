import asyncio
from pathlib import Path
from lidar_downloader import LidarDownloader, DownloaderConfig, TileDownloader
from tile_downloader import download_lidar_dsm

# Example 1: Using the general downloader
async def download_lidar():
    config = DownloaderConfig(
        output_dir=Path("./data"),
        browser_type="chrome",
        headless=True,
        year="2023"
    )

    downloader = LidarDownloader(config)
    await downloader.download_tiles(
        shapefile_path=Path("your_area.shp"),
        products=["LIDAR Tiles DSM"]
    )

# Example 2: Using the tile-specific downloader
async def download_specific_tile():
    tile_downloader = TileDownloader()

    # Option 1: Direct tile download
    await tile_downloader.download_tile(
        product="national_lidar_programme_dsm",
        year="2023",
        tile_key="ST57SW",
        output_dir=Path("./data")
    )

    # Option 2: Get tile key from coordinates
    osgb_grid = Path("osgb_grid.parquet")  # or GeoDataFrame
    tile_key = tile_downloader.get_tile_key_from_coords(
        x=123456,  # OSGB coordinates
        y=789012,
        osgb_grid=osgb_grid
    )

    # Create shapefile for the tile
    zip_path = TileDownloader.create_tile_shapefile(
        tile_key=tile_key,
        osgb_grid=osgb_grid,
        output_dir=Path("./data")
    )

# specific tile downloader
async def tile_downloader():
    os_grids = './data/os_bng_grids.parquet'
    await download_lidar_dsm('ST68NW',
                   parquet_path=os_grids,
                   output_dir='downloads',
                   verbose=True)




# Run the examples
if __name__ == "__main__":
    # asyncio.run(download_lidar())
    asyncio.run(tile_downloader())