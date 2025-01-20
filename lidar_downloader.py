import os
import re
import uuid
import shutil
import glob
import time
import tempfile
import logging
from pathlib import Path
from dataclasses import dataclass
from typing import List, Optional, Dict, Any, Union
import pandas as pd
from contextlib import contextmanager
from functools import wraps
import geopandas as gp
from zipfile import ZipFile
import argparse
import urllib.request
from tqdm.auto import tqdm

from selenium import webdriver
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    NoSuchElementException,
    ElementNotInteractableException,
    TimeoutException,
    WebDriverException
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
DEFRA_URL = "https://environment.data.gov.uk/DefraDataDownload/?Mode=survey"
MAX_VERTICES = 1000
DEFAULT_TIMEOUT = 300
AVAILABLE_PRODUCTS = {
    "dsm": "LIDAR Tiles DSM",
    "dtm": "LIDAR Tiles DTM",
    "point_cloud": "LIDAR Point Cloud",
    "national": "National LIDAR Programme Point Cloud"
}

@dataclass
class DownloaderConfig:
    """Configuration for the LIDAR downloader."""
    output_dir: Path
    browser_type: str = "chrome"
    headless: bool = True
    timeout: int = DEFAULT_TIMEOUT
    year: str = "latest"
    all_years: bool = False
    print_only: bool = False
    verbose: bool = False

class DownloadError(Exception):
    """Custom exception for download errors."""
    pass

class BrowserError(Exception):
    """Custom exception for browser-related errors."""
    pass

def retry_on_exception(retries: int = 3, delay: int = 1):
    """Decorator for retrying operations."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            for i in range(retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    logger.warning(f"Attempt {i+1} failed: {str(e)}")
                    if i < retries - 1:
                        time.sleep(delay * (i + 1))  # Exponential backoff
            raise last_exception
        return wrapper
    return decorator

class DownloadProgressBar(tqdm):
    """Custom progress bar for downloads."""
    def update_to(self, b: int = 1, bsize: int = 1, tsize: Optional[int] = None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)

class LidarDownloader:
    """Main class for downloading LIDAR data."""

    def __init__(self, config: DownloaderConfig):
        self.config = config
        self.driver = None
        self.wait = None
        self._setup_directories()

    def _setup_directories(self):
        """Create necessary directories."""
        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir = Path(tempfile.mkdtemp())
        self.temp_name = str(uuid.uuid4())

    def _setup_browser(self):
        """Initialize the web browser."""
        try:
            if self.config.browser_type.lower() == "firefox":
                from selenium.webdriver.firefox.options import Options
                options = Options()
                options.headless = self.config.headless
                self.driver = webdriver.Firefox(options=options)
            else:
                from selenium.webdriver.chrome.options import Options
                from selenium.webdriver.chrome.service import Service
                from webdriver_manager.chrome import ChromeDriverManager

                options = Options()
                options.headless = self.config.headless
                if self.config.headless:
                    options.add_argument("--disable-gpu")
                    options.add_argument("--no-sandbox")
                    options.add_argument("--disable-dev-shm-usage")

                self.driver = webdriver.Chrome(
                    service=Service(ChromeDriverManager().install()),
                    options=options
                )

            self.wait = WebDriverWait(self.driver, self.config.timeout)
            self.driver.set_window_size(1920, 1080)

        except WebDriverException as e:
            raise BrowserError(f"Failed to initialize browser: {str(e)}")

    async def download_url(self, url: str, output_path: Path):
        """Download a file with progress bar."""
        try:
            with DownloadProgressBar(
                unit='B', unit_scale=True, miniters=1, desc=url.split('/')[-1]
            ) as t:
                urllib.request.urlretrieve(
                    url,
                    filename=output_path,
                    reporthook=t.update_to
                )
        except Exception as e:
            raise DownloadError(f"Failed to download {url}: {str(e)}")

    def _wait_and_click(self, selector: str, timeout: Optional[int] = None):
        """Wait for element to be clickable and click it."""
        timeout = timeout or self.config.timeout
        try:
            element = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
            )
            element.click()
            return element
        except TimeoutException:
            raise BrowserError(f"Element {selector} not clickable after {timeout} seconds")

    def process_shapefile(self, shapefile_path: Path) -> List[Path]:
        """Process input shapefile and prepare for upload."""
        try:
            shp = gp.read_file(shapefile_path)

            # Check CRS
            if shp.crs is None:
                raise ValueError("Shapefile has no coordinate reference system defined")

            # Transform to OSGB if necessary
            if shp.crs != "EPSG:27700":
                shp = shp.to_crs("EPSG:27700")

            # Handle complex geometries
            if shp.area.values[0] > 561333677 or len(shp.explode(index_parts=True)) > 1:
                return self._tile_large_geometry(shp)

            # Simplify if too many vertices
            if self._count_vertices(shp) > MAX_VERTICES:
                shp = self._simplify_geometry(shp)

            # Save processed shapefile
            output_shp = self.temp_dir / f"{self.temp_name}.shp"
            shp.to_file(output_shp)

            return [self._create_zip_from_shapefile(output_shp)]

        except Exception as e:
            raise ValueError(f"Failed to process shapefile: {str(e)}")

    def _count_vertices(self, shp: gp.GeoDataFrame) -> int:
        """Count vertices in geometry."""
        count = 0
        for _, row in shp.iterrows():
            geom = row.geometry
            if geom.type.startswith("Multi"):
                for part in geom:
                    count += len(part.exterior.coords)
            else:
                count += len(geom.exterior.coords)
        return count

    def _simplify_geometry(self, shp: gp.GeoDataFrame) -> gp.GeoDataFrame:
        """Simplify geometry to reduce vertex count."""
        tolerance = 10
        while self._count_vertices(shp) > MAX_VERTICES:
            shp.geometry = shp.simplify(tolerance)
            tolerance *= 2
            if tolerance > 1000:  # Safety check
                raise ValueError("Could not simplify geometry sufficiently")
        return shp

    def _create_zip_from_shapefile(self, shapefile_path: Path) -> Path:
        """Create ZIP file from shapefile and associated files."""
        zip_path = self.temp_dir / f"{shapefile_path.stem}.zip"
        with ZipFile(zip_path, 'w') as zip_obj:
            for ext in ['.shp', '.shx', '.dbf', '.prj']:
                file_path = shapefile_path.with_suffix(ext)
                if file_path.exists():
                    zip_obj.write(file_path, file_path.name)
        return zip_path

    async def download_tiles(self, shapefile_path: Path, products: List[str]):
        """Main method to download LIDAR tiles."""
        try:
            self._setup_browser()
            self.driver.get(DEFRA_URL)

            # Process shapefile
            zip_files = self.process_shapefile(shapefile_path)

            for zip_file in zip_files:
                await self._process_single_zip(zip_file, products)

        except Exception as e:
            logger.error(f"Download failed: {str(e)}")
            raise
        finally:
            self.cleanup()

    async def _process_single_zip(self, zip_file: Path, products: List[str]):
        """Process a single ZIP file for download."""
        try:
            # Upload shapefile
            self._wait_and_click(".fswiLB select")
            select = Select(self.driver.find_element(By.CSS_SELECTOR, ".fswiLB select"))
            select.select_by_value("Upload shapefile")

            # Upload file
            upload_input = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".shapefile-upload input"))
            )
            upload_input.send_keys(str(zip_file))

            # Process products
            for product in products:
                await self._process_product(product)

        except Exception as e:
            logger.error(f"Failed to process {zip_file}: {str(e)}")
            raise

    @retry_on_exception(retries=3)
    async def _process_product(self, product: str):
        """Process a single product for download."""
        try:
            # Select product
            product_select = Select(self.driver.find_element(By.CSS_SELECTOR, "#productSelect"))
            product_select.select_by_visible_text(product)

            # Handle years
            year_select = Select(self.driver.find_element(By.CSS_SELECTOR, "#yearSelect"))
            years = [x.get_attribute('value') for x in year_select.options]

            selected_years = self._get_years_to_download(years)

            for year in selected_years:
                await self._download_year_data(product, year)

        except Exception as e:
            logger.error(f"Failed to process product {product}: {str(e)}")
            raise

    def _get_years_to_download(self, available_years: List[str]) -> List[str]:
        """Determine which years to download based on configuration."""
        if self.config.year == "latest":
            return [available_years[0]]
        elif self.config.all_years:
            return [y for y in available_years if int(y) >= int(self.config.year)]
        elif self.config.year in available_years:
            return [self.config.year]
        else:
            logger.warning(f"Requested year {self.config.year} not available")
            return []

    def cleanup(self):
        """Clean up resources."""
        if self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                logger.warning(f"Failed to quit driver: {str(e)}")

        try:
            shutil.rmtree(self.temp_dir)
        except Exception as e:
            logger.warning(f"Failed to remove temp directory: {str(e)}")

class TileDownloader:
    """Class for handling direct tile downloads."""

    def __init__(self, subscription_key: str = "public"):
        self.base_url = "https://api.agrimetrics.co.uk/tiles/collections/survey"
        self.subscription_key = subscription_key

    def _construct_url(self, product: str, year: str, tile_key: str) -> str:
        """Construct the download URL for a specific tile."""
        return f"{self.base_url}/{product}/{year}/1/{tile_key}?subscription-key={self.subscription_key}"

    def _validate_tile_key(self, tile_key: str) -> bool:
        """Validate the format of a tile key."""
        pattern = r'^[A-Z]{2}\d{4}'
        return bool(re.match(pattern, tile_key))

    async def download_tile(self, product: str, year: str, tile_key: str, output_dir: Path) -> Path:
        """Download a specific tile."""
        # if not self._validate_tile_key(tile_key):
            # raise ValueError(f"Invalid tile key format: {tile_key}")

        url = self._construct_url(product, year, tile_key)
        output_path = Path(output_dir) / f"{product}_{year}_{tile_key}.tif"
        os.makedirs(output_dir, exist_ok=True)

        print(f"Downloading Tile {tile_key} from url: {url}")

        try:
            with DownloadProgressBar(
                unit='B', unit_scale=True, miniters=1, desc=f"Downloading {tile_key}"
            ) as t:
                urllib.request.urlretrieve(
                    url,
                    filename=output_path,
                    reporthook=t.update_to
                )
            return output_path
        except Exception as e:
            raise DownloadError(f"Failed to download tile {tile_key}: {str(e)}")

    def get_tile_key_from_coords(self, x: float, y: float, osgb_grid: Union[gp.GeoDataFrame, Path]) -> str:
        """Get tile key from coordinates using OSGB grid."""
        if isinstance(osgb_grid, Path):
            if osgb_grid.suffix == '.parquet':
                osgb_grid = gp.read_parquet(osgb_grid)
            else:
                osgb_grid = gp.read_file(osgb_grid)

        point = gp.GeoDataFrame(
            geometry=[gp.points_from_xy([x], [y])[0]],
            crs="EPSG:27700"
        )

        # Spatial join to find containing tile
        joined = gp.sjoin(point, osgb_grid, how="left", predicate="within")

        if joined.empty or pd.isna(joined.index_right[0]):
            raise ValueError(f"No tile found for coordinates ({x}, {y})")

        return osgb_grid.iloc[joined.index_right[0]]['tile_key']

    @staticmethod
    def create_tile_shapefile(tile_key: str, osgb_grid: Union[gp.GeoDataFrame, Path], output_dir: Optional[Path] = None) -> Path:
        """Create a shapefile for a specific tile."""
        if isinstance(osgb_grid, Path):
            if osgb_grid.suffix == '.parquet':
                osgb_grid = gp.read_parquet(osgb_grid)
            else:
                osgb_grid = gp.read_file(osgb_grid)

        # Extract tile geometry
        tile_geom = osgb_grid[osgb_grid['tile_key'] == tile_key]
        if tile_geom.empty:
            raise ValueError(f"Tile key {tile_key} not found in OSGB grid")

        # Create temporary directory if output_dir not provided
        if output_dir is None:
            output_dir = Path(tempfile.mkdtemp())
        else:
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)

        # Save shapefile
        shp_path = output_dir / f"{tile_key}.shp"
        tile_geom.to_file(shp_path)

        # Create zip file
        zip_path = output_dir / f"{tile_key}.zip"
        with ZipFile(zip_path, 'w') as zip_obj:
            for ext in ['.shp', '.shx', '.dbf', '.prj']:
                file_path = shp_path.with_suffix(ext)
                if file_path.exists():
                    zip_obj.write(file_path, file_path.name)

        return zip_path

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Download LIDAR data from DEFRA")
    parser.add_argument("shapefile", type=Path, help="Path to input shapefile")
    parser.add_argument("--output-dir", type=Path, default=Path("."),
                      help="Output directory for downloads")
    parser.add_argument("--browser", choices=["chrome", "firefox"],
                      default="chrome", help="Browser to use")
    parser.add_argument("--year", default="latest",
                      help="Year of data to download")
    parser.add_argument("--all-years", action="store_true",
                      help="Download all available years")
    parser.add_argument("--products", nargs="+", choices=AVAILABLE_PRODUCTS.keys(),
                      required=True, help="Products to download")
    parser.add_argument("--headless", action="store_true",
                      help="Run browser in headless mode")
    parser.add_argument("--verbose", action="store_true",
                      help="Enable verbose logging")

    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    config = DownloaderConfig(
        output_dir=args.output_dir,
        browser_type=args.browser,
        headless=args.headless,
        year=args.year,
        all_years=args.all_years,
        verbose=args.verbose
    )

    products = [AVAILABLE_PRODUCTS[p] for p in args.products]

    try:
        downloader = LidarDownloader(config)
        import asyncio
        asyncio.run(downloader.download_tiles(args.shapefile, products))
    except Exception as e:
        logger.error(f"Download failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()