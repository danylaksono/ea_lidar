import os
import uuid
import tempfile
import aiohttp
import geopandas as gpd
import time
import re
import glob
import urllib.request
import pyarrow
import asyncio
import shutil

from zipfile import ZipFile
from typing import Union, List
from tqdm.auto import tqdm
from selenium import webdriver
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# COGEO
import boto3
import rasterio
import tempfile
from rasterio.shutil import copy
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles


# COG Converter
def convert_cog(input_file: str, output_file: str, verbose: bool = True) -> None:
    """
    Converts a GeoTIFF to a Cloud Optimized GeoTIFF (COG) using rio-cogeo.
    """
    print("  :: COG Conversion ::")
    cog_profile = cog_profiles.get("deflate")
    config = {
        "GDAL_NUM_THREADS": "4",
        "GDAL_TIFF_INTERNAL_MASK": True,
        "GDAL_TIFF_OVR_BLOCKSIZE": "128",
    }

    if verbose:
            print(f" ...Starting COG conversion for {input_file} -> {output_file}")
    try:
        cog_translate(
            input_file,
            output_file,
            cog_profile,
            config=config,
            in_memory=False,
            nodata=None,  # Adjust nodata value
            quiet=False,
        )
        print(f" Finished COG conversion for {input_file} -> {output_file}")
    except Exception as e:
        print(f" Error converting {input_file} to COG: {e}")


class DownloadProgressBar(tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)

async def download_file(url: str, output_path: str, session: aiohttp.ClientSession):
    """Asynchronously download a file from URL with progress bar"""
    async with session.get(url) as response:
        if response.status == 200:
            total_size = int(response.headers.get('content-length', 0))
            with tqdm(total=total_size, unit='iB', unit_scale=True) as pbar:
                with open(output_path, 'wb') as f:
                    async for data in response.content.iter_chunked(1024):
                        size = f.write(data)
                        pbar.update(size)
        else:
            print(f"Failed to download {url}. Status code: {response.status}")

def setup_browser():
    try:
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')

        # Fix: Use install() instead of driver_path
        service = Service(ChromeDriverManager().install())
        browser = webdriver.Chrome(service=service, options=chrome_options)
        return browser
    except Exception as e:
        print(f"Error setting up Chrome browser: {e}")
        raise

async def download_lidar_dsm(tile_names: Union[str, List[str]],
                      parquet_path: str,
                      output_dir: str = '.',
                      verbose: bool = True,
                      product: str = 'national_lidar_programme_dsm',
                      max_retries: int = 1) -> None:
    """
    Download National LIDAR Programme DSM data for specified tile names.

    Args:
        tile_names: Single tile name or list of tile names
        parquet_path: Path to geoparquet file containing tile geometries
        output_dir: Directory to save downloaded files
        verbose: Print progress messages
        year: Year of data to download ('latest' or specific year)
        max_retries: Maximum number of retries for downloading

        List of products:
         - lidar_composite_dtm
         - lidar_composite_first_return_dsm
         - lidar_composite_last_return_dsm
         - lidar_point_cloud
         - lidar_tiles_dsm
         - lidar_tiles_dtm
         - national_lidar_programme_dsm  (default)
         - national_lidar_programme_dtm
         - national_lidar_programme_first_return_dsm
         - national_lidar_programme_intensity
         - national_lidar_programme_point_cloud
         - national_lidar_programme_vom
         - vertical_aerial_photography_tiles_night_time
    """
    # Convert single tile name to list
    if isinstance(tile_names, str):
        tile_names = [tile_names]

    # Create temp directory
    tmp_dir = tempfile.mkdtemp()

    # Read parquet file
    if verbose:
        print(f"Reading geometries from {parquet_path}")
    gdf = gpd.read_parquet(parquet_path)

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    async with aiohttp.ClientSession() as session:
        for tile_name in tile_names:
            if verbose:
                print(f"\nProcessing tile: {tile_name}")

            # Get geometry for tile
            # tile_geom = gdf[gdf['tile_name'] == tile_name]
            # if len(tile_geom) == 0:
            #     print(f"Warning: Tile {tile_name} not found in parquet file")
            #     continue

            # expand the tile to include the touching tiles
            current_tile = gdf[gdf['tile_name'] == tile_name]
            print(f"Current tile: {current_tile}")

            if len(current_tile) == 0:
                print(f"Warning: Tile {tile_name} not found in parquet file")
                continue

            # get the touching tiles
            neighbors = gdf[gdf.geometry.touches(current_tile.unary_union)]
            print(f"Neighbors: {neighbors}")

            merged_geom = current_tile.geometry.unary_union.union(neighbors.geometry.unary_union)
            print(f"Merged geometry: {merged_geom}")

            # Convert to GeoDataFrame
            merged_gdf = gpd.GeoDataFrame(geometry=[merged_geom], crs=gdf.crs)

            # Create unique temp name
            tmp_name = str(uuid.uuid4())
            os.makedirs(tmp_dir, exist_ok=True)
            shp_path = os.path.join(tmp_dir, f"{tmp_name}.shp")

            # Save to shapefile
            # tile_geom.to_file(shp_path)
            print(f"Saving merged geometry to {shp_path}")
            merged_gdf.to_file(shp_path)

            # Create zip file
            print(f"Creating zip file for {shp_path}")
            zip_path = os.path.join(tmp_dir, f"{tmp_name}.zip")
            with ZipFile(zip_path, 'w') as zipObj:
                for f in glob.glob(os.path.join(tmp_dir, f"{tmp_name}.*")):
                    zipObj.write(f, os.path.basename(f))

            if verbose:
                print(f"Created temporary files in {tmp_dir}")

            retry_count = 0
            while retry_count < max_retries:
                try:
                    browser = setup_browser()
                    # Access DEFRA data download page
                    if verbose:
                        print("Accessing DEFRA data download page...")
                    browser.get("https://environment.data.gov.uk/DefraDataDownload/?Mode=survey")
                    if verbose:
                        print("Navigated to DEFRA data download page.")
                    wait = WebDriverWait(browser, 300)

                    # Wait for upload option
                    if verbose:
                        print("Waiting for upload option to be present...")
                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".fswiLB select")))
                    if verbose:
                        print("Upload option found.")
                    select_element = Select(browser.find_element(By.CSS_SELECTOR, ".fswiLB select"))
                    if verbose:
                        print("Selecting 'Upload shapefile' option.")
                    select_element.select_by_value("Upload shapefile")

                    # Upload shapefile
                    if verbose:
                        print("Waiting for shapefile upload input...")
                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".shapefile-upload input")))
                    if verbose:
                        print(f"Uploading shapefile from {zip_path}...")
                    browser.find_element(By.CSS_SELECTOR, ".shapefile-upload input").send_keys(zip_path)
                    if verbose:
                        print("Shapefile uploaded.")

                    # Click Get Tile Selector
                    if verbose:
                        print("Clicking 'Get Tile Selector' button...")
                    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".download-button")))
                    browser.find_element(By.CSS_SELECTOR, ".download-button").click()
                    if verbose:
                        print("'Get Tile Selector' button clicked.")


                    # click select product dropdown
                    if verbose:
                        print("Clicking product dropdown...")
                    wait.until(EC.element_to_be_clickable((By.XPATH, "//label[text()='Select product']/following-sibling::select")))
                    select_element = browser.find_element(By.XPATH, "//label[text()='Select product']/following-sibling::select")
                    select = Select(select_element)

                    # Print available option values
                    for option in select.options:
                        print(f"Option value: {option.get_attribute('value')}")

                    # Select the desired option - use 'national_lidar_programme_dsm' by default
                    select.select_by_value(product)
                    if verbose:
                        print(f"Product selected: {product}")

                    # Click and select year dropdown
                    if verbose:
                        print("Clicking year dropdown...")
                    wait.until(EC.element_to_be_clickable((By.XPATH, "//label[text()='Select year']/following-sibling::select")))
                    year_select_element = browser.find_element(By.XPATH, "//label[text()='Select year']/following-sibling::select")
                    year_select = Select(year_select_element)

                    # Store available year options
                    year_options = [option.get_attribute('value') for option in year_select.options]

                    # Iterate through each year option
                    for year in year_options:
                        # Select the year
                        year_select.select_by_value(year)
                        if verbose:
                            print(f"Year selected: {year}")

                        # Wait for and list all available tiles
                        if verbose:
                            print(f' ...listing all available tiles in year {year}')
                        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".tiles-list a")))
                        links = browser.find_elements(By.CSS_SELECTOR, ".tiles-list a")

                        # Store matching products and links
                        matching_products = []

                        for link in links:
                            href = link.get_attribute("href")
                            name = link.text
                            if verbose:
                                print(f"    Product: {name}")
                                print(f"    Link: {href}")

                            # Check if current tile_name matches the product name (case-insensitive)
                            if re.search(tile_name, name, re.IGNORECASE):
                                matching_products.append((name, href))

                        if matching_products:
                            if verbose:
                                print(f"\n Found {len(matching_products)} matching products for tile {tile_name} in year {year}:")
                                for name, href in matching_products:
                                    print(f"    Matching Product: {name}")
                                    print(f"    Matching Link: {href}")

                            for name, href in matching_products:
                                print(f"\n ...Processing product: {name}")
                                # Create output directory for the tile
                                tile_output_dir = os.path.join(output_dir, f"{tile_name}")
                                os.makedirs(tile_output_dir, exist_ok=True)

                                # Define temporary zip path
                                temp_zip_path = os.path.join(tmp_dir, f"{name}.zip")
                                os.makedirs(os.path.dirname(temp_zip_path), exist_ok=True)

                                if os.path.exists(temp_zip_path):
                                    if verbose:
                                        print(f"\n ...File {temp_zip_path} already exists. Skipping download.")
                                    continue
                                if verbose:
                                    print(f"\n ...Downloading {name} to {temp_zip_path}")
                                await download_file(href, temp_zip_path, session)

                                # Extract zip file to temporary directory
                                print(" ...Extracting zip file to temporary directory...")
                                # os.makedirs(os.path.join(tmp_dir, name), exist_ok=True)
                                with ZipFile(temp_zip_path, 'r') as zip_ref:
                                    zip_ref.extractall(tmp_dir)

                                # Find the extracted TIFF file
                                print(" ...Searching for TIFF file in extracted folder...")
                                tiff_files = glob.glob(os.path.join(tmp_dir, "*.tif"))
                                if not tiff_files:
                                    print(f" ...No TIFF file found in {temp_zip_path}")
                                    continue
                                tiff_file = tiff_files[0]
                                print(f" ...Found TIFF file: {tiff_file}")

                                # Convert TIFF to COG
                                print(" ...Converting TIFF to COG")
                                cog_output_path = os.path.join(tile_output_dir, f"cog_{tile_name}.tif")
                                try:
                                    convert_cog(tiff_file, cog_output_path)
                                    print(f" ---- COG file saved to {cog_output_path}")
                                except Exception as e:
                                    print(f" ---- Error converting {tiff_file} to COG: {e}")

                                # Cleanup temporary zip and extracted files
                                print(" ...Cleaning up temporary extracted files...")
                                os.remove(temp_zip_path)
                                extracted_folder = os.path.splitext(os.path.basename(tiff_file))[0]
                                shutil.rmtree(os.path.join(tmp_dir, extracted_folder), ignore_errors=True)
                            break  # Exit year loop if successful

                        else:
                            if verbose:
                                print(f"\n No matching products found for tile {tile_name} in year {year}")
                            continue

                    else: # No matching products found in any year
                        if verbose:
                            print(f"\nNo matching products found for tile {tile_name}")
                        raise Exception(f"No matching products found for tile {tile_name}")
                            # print("Downloading all listed tiles instead.")
                        # Create output directory for the tile
                        # tile_output_dir = os.path.join(output_dir, f"{tile_name}")
                        # os.makedirs(tile_output_dir, exist_ok=True)

                        # # Download all listed tiles
                        # for link in links:
                        #     href = link.get_attribute("href")
                        #     name = link.text
                        #     output_file = os.path.join(tile_output_dir, f"{name}.zip")
                        #     if os.path.exists(output_file):
                        #         if verbose:
                        #             print(f"\nFile {output_file} already exists. Skipping download.")
                        #         continue
                        #     if verbose:
                        #         print(f"\nDownloading {name} to {output_file}")
                        #     await download_file(href, output_file, session)

                    break  # Exit retry loop if successful

                except Exception as e:
                    retry_count += 1
                    if retry_count == max_retries:
                        print(f"Failed after {max_retries} attempts: {str(e)}")
                        raise
                    print(f"Attempt {retry_count} failed. Retrying...")
                    time.sleep(5)  # Wait before retry

                finally:
                    if 'browser' in locals():
                        browser.quit()
                    await asyncio.sleep(2)  # Cool down period

    # Cleanup temp directory
    shutil.rmtree(tmp_dir)