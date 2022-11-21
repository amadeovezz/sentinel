# Sentinel 2

### About Sentinel 2 Images

- imagery located in s3
- 13 bands listed in directories
  - example path: `tiles/1/X/CA/2022/10/10/0/`
  - path is specified by 'tile id' ('UTM', 'latitude', and 'square') and date
  - file types are `.jp2`
  - interested in blue `BO2.jp2`, green `BO2.jp2`, and red `BO4.jp2` bands

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

`python3 -m venv venv`

`source venv/bin/activate`

`pip install -r requirements.txt`


**Unit**

From root dir: `python -m pytest tests/unit/`

**Functional**

Must have aws keys in: `'./keys/aws.json'`

From root dir: `python -m pytest tests/functional/`

### Classes


#### RGBPuller

This class:

1. Searches for valid images to pull from s3.

Searches the entire bucket on the bucket prefix: `tile_id`. Which has the path: `tiles/UTM/lattitude/square/`. This is quite slow, because boto3 does
not provide any efficient way to perform server side filtering other than `prefix` and `delimeter`. 

2. Downloads images.

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

- Improve memory and cpu performance for median processing:
  - Investigate building a BlockImageProcessor (with raterio block reads and writes). Re-assembly logic may be challenging here.
- Compute hash on multi-part download to ensure integrity of file
- Look into scaling intensity values. I am using QGIS to view the output image, but this automatically rescales the intensity values so the image can be displayed correctly. JP2 is a lossy format by default

## How to run

WIP: troubleshooting docker... Coming shortly 