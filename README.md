# ea_lidar

Python code to bulk download UK Environment Agency LiDAR data

Updated from [https://github.com/philwilkes/ea_lidar](https://github.com/philwilkes/ea_lidar) to deal with the updated DEFRA Lidar downloader.

Improved to allow for downloading tiles by name.
It automatically grabs the tile from OS 5km, converted to zipped shapefile and proceed with downloading.


## Preparation

Install the necessary `webdriver-manager` to match updated chrome driver API:

```
pip install webdriver-manager --upgrade
```

Install other dependencies:

```bash
pip install -r requirements.txt
```

## Usage

See examples in `example.py`