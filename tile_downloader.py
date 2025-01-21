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

async def download_lidar_dsm(tile_names: Union[str, List[str]],
                      parquet_path: str,
                      output_dir: str = '.',
                      verbose: bool = True,
                      year: str = 'latest') -> None:
    """
    Download National LIDAR Programme DSM data for specified tile names.

    Args:
        tile_names: Single tile name or list of tile names
        parquet_path: Path to geoparquet file containing tile geometries
        output_dir: Directory to save downloaded files
        verbose: Print progress messages
        year: Year of data to download ('latest' or specific year)
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
            tile_geom = gdf[gdf['tile_name'] == tile_name]
            if len(tile_geom) == 0:
                print(f"Warning: Tile {tile_name} not found in parquet file")
                continue

            # Create unique temp name
            tmp_name = str(uuid.uuid4())
            shp_path = os.path.join(tmp_dir, f"{tmp_name}.shp")

            # Save to shapefile
            tile_geom.to_file(shp_path)

            # Create zip file
            zip_path = os.path.join(tmp_dir, f"{tmp_name}.zip")
            with ZipFile(zip_path, 'w') as zipObj:
                for f in glob.glob(os.path.join(tmp_dir, f"{tmp_name}.*")):
                    zipObj.write(f, os.path.basename(f))

            if verbose:
                print(f"Created temporary files in {tmp_dir}")

            # Setup Chrome
            options = Options()
            options.headless = True
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),
                                    options=options)

            try:
                # Access DEFRA data download page
                if verbose:
                    print("Accessing DEFRA data download page...")
                driver.get("https://environment.data.gov.uk/DefraDataDownload/?Mode=survey")
                if verbose:
                    print("Navigated to DEFRA data download page.")
                wait = WebDriverWait(driver, 300)

                # Wait for upload option
                if verbose:
                    print("Waiting for upload option to be present...")
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".fswiLB select")))
                if verbose:
                    print("Upload option found.")
                select_element = Select(driver.find_element(By.CSS_SELECTOR, ".fswiLB select"))
                if verbose:
                    print("Selecting 'Upload shapefile' option.")
                select_element.select_by_value("Upload shapefile")

                # Upload shapefile
                if verbose:
                    print("Waiting for shapefile upload input...")
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".shapefile-upload input")))
                if verbose:
                    print(f"Uploading shapefile from {zip_path}...")
                driver.find_element(By.CSS_SELECTOR, ".shapefile-upload input").send_keys(zip_path)
                if verbose:
                    print("Shapefile uploaded.")

                # Click Get Tile Selector
                if verbose:
                    print("Clicking 'Get Tile Selector' button...")
                wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".download-button")))
                driver.find_element(By.CSS_SELECTOR, ".download-button").click()
                if verbose:
                    print("'Get Tile Selector' button clicked.")


                # click select product dropdown
                if verbose:
                    print("Clicking product dropdown...")
                wait.until(EC.element_to_be_clickable((By.XPATH, "//label[text()='Select product']/following-sibling::select")))
                select_element = driver.find_element(By.XPATH, "//label[text()='Select product']/following-sibling::select")
                select = Select(select_element)

                # Print available option values
                for option in select.options:
                    print(f"Option value: {option.get_attribute('value')}")

                # Select the desired option
                select.select_by_value("national_lidar_programme_dsm")
                if verbose:
                    print("Product selected: national_lidar_programme_dsm")

                # Wait for and list all available tiles
                if verbose:
                    print('...listing all available tiles')
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".tiles-list a")))
                links = driver.find_elements(By.CSS_SELECTOR, ".tiles-list a")

                # Store matching products and links
                matching_products = []

                for link in links:
                    href = link.get_attribute("href")
                    name = link.text
                    if verbose:
                        print(f"Product: {name}")
                        print(f"Link: {href}")

                    # Check if current tile_name matches the product name (case-insensitive)
                    if re.search(tile_name, name, re.IGNORECASE):
                        matching_products.append((name, href))

                if matching_products:
                    if verbose:
                        print(f"\nFound {len(matching_products)} matching products for tile {tile_name}:")
                        for name, href in matching_products:
                            print(f"Matching Product: {name}")
                            print(f"Matching Link: {href}")

                    # Download each matching product
                    for name, href in matching_products:
                        # Create output directory for the tile
                        tile_output_dir = os.path.join(output_dir, f"{tile_name}")
                        os.makedirs(tile_output_dir, exist_ok=True)
                        output_file = os.path.join(tile_output_dir, f"{name}.zip")
                        if os.path.exists(output_file):
                            if verbose:
                                print(f"\nFile {output_file} already exists. Skipping download.")
                            continue
                        if verbose:
                            print(f"\nDownloading {name} to {output_file}")
                        await download_file(href, output_file, session)

                else:
                    if verbose:
                        print(f"\nNo matching products found for tile {tile_name}")
                        print("Downloading all listed tiles instead.")
                    # Create output directory for the tile
                    tile_output_dir = os.path.join(output_dir, f"{tile_name}")
                    os.makedirs(tile_output_dir, exist_ok=True)

                    # Download all listed tiles
                    for link in links:
                        href = link.get_attribute("href")
                        name = link.text
                        output_file = os.path.join(tile_output_dir, f"{name}.zip")
                        if os.path.exists(output_file):
                            if verbose:
                                print(f"\nFile {output_file} already exists. Skipping download.")
                            continue
                        if verbose:
                            print(f"\nDownloading {name} to {output_file}")
                        await download_file(href, output_file, session)

            finally:
                if verbose:
                    print("Closing the browser.")
                driver.quit()
                if verbose:
                    print("Browser closed.")
                await asyncio.sleep(10)  # 10 seconds delay between tiles

    # Cleanup temp directory
    import shutil
    shutil.rmtree(tmp_dir)