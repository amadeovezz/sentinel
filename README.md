# Sentinel 2

### About Sentinel 2 Images

- imagery located in s3
- 13 bands listed in directories
  - example path: `tiles/1/X/CA/2022/10/10/0/`
  - path is specified by 'tile id' ('UTM', 'latitude', and 'square') and date
  - file types are `.jp2`
  - interested in blue `BO2.jp2`, green `BO2.jp2`, and red `BO4.jp2` bands

## Example output:

Output after running: 

`python s2_imagery_retriever.py 10UDV 2019-08-26T02:44:33.000000Z 2019-09-07T18:42:22.000000Z combined_image.tif` 

Located in `images/final/combined_image.tiff`

Short tutorial and alternative output: check out `program-output.md`

## Program details 

- Query for valid files on s3 using tile id and date
- Once files are found, concurrently download the necessary jp2 files in 3 directories (grouped by band):
  - The defaults are:
  - `./tmp/red/`
  - `./tmp/green/`
  - `./tmp/blue/`
- Use rasterio windowing to read all files into memory - window by window - and compute median (in parrallel)
- Create composite image

### Libraries Used

- `boto3`: as a s3 client 
- `numpy`: for arrays and computation 
- `rasterio`: reading/writing jp2 files
- `click`: for creating a cli

### Tests


**Set-up**

- `git clone https://github.com/amadeovezz/sentinel`
- `cd` into sentinel
- `python3 -m venv .`
- `source ./bin/activate.fish`
- `pip3 install -r requirements.txt`

**Unit**

From root dir: `python -m pytest tests/unit/`

**Functional**

Note: Must have aws keys configured in home directory

From root dir: `python -m pytest tests/functional/`

### Classes


#### RGBPuller

This class:

1. Searches for valid images to pull from s3. Filters images based on a time range using the file' `LAST_MODIFIED` timestamp.

Searches the entire bucket on the bucket prefix: `tile_id`. Which has the path: `tiles/UTM/lattitude/square/`. This is quite slow, because boto3 does
not provide any efficient way to perform server side filtering other than `prefix` and `delimeter`. 

2. Download's images.

This is also quite slow due to the size of the files, however, we can leverage threading here, and concurrently download each red,blue,green file accordingly. Multi-part downloading is also used to speed up this process.

#### WindowImageProcessor

This class:

1. Reads in files that were downloaded by RGBPuller() and leverages rasterio windowing
to compute the median across multiple different timestamps of the files. This is performed in parrallel for each band.

2. Writes the result of each band to a composite image.

Notes:

1. Accessing the contents of a `jp2` file (a ndarray) into memory via rasterio's `f.read(1)`, is very slow.
2. Manipulating all the images in memory does not scale - especially if we want to parallelize the processing of each band.  

### Future Improvements

- Improve memory and cpu performance for image processing:
  - Investigate building a BlockImageProcessor (with raterio block reads and writes). Re-assembly logic may be challenging here.
- Compute hash on multi-part download to ensure integrity of file
- The intensity values in the GeoTiff appear to be slightly off. Look into scaling / re-sampling.

## How to run

- `cd` (go to home directory)
- `git clone https://github.com/amadeovezz/sentinel`
- `mv sentinel scratch`
- `docker run -it -v $HOME/scratch/:/scratch/ -v $HOME/.aws:/root/.aws/ sentinelhub/eolearn bash` (note $HOME is for fish shells)
- `cd /scratch`
- `python s2_mosaicker.py` 
    - command line args default to:
      - tile_id: `10UDV` 
      - start: `2019-08-26T02:44:33.000000Z`
      - end: `2019-09-07T18:42:22.000000Z`
      - output path: `./tmp/final/combined_image.tiff`
- To view options:
  - `python s2_mosaicker --help`

Once the program is finished running, the blue,red,green band images are located in:

- `/scratch/tmp/red/`
- `/scratch/tmp/green/`
- `/scratch/tmp/blue/`

And the final tiff:

- `/scratch/tmp/final/`

If you wish to re-run the program without re-downloading the images:

- `python s2_mosaicker --has_pulled`