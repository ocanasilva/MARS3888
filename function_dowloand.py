import os
import requests
from datetime import datetime, timedelta


def download_netcdf_ereefs(
    base_url,
    download_dir,
    start_year,
    start_month,
    end_year,
    end_month,
    file_prefix,
    file_suffix=".nc",
    date_format="%Y-%m-%d",
    start_day=None,
    end_day=None,
    verbose=True
):
    """
    Generic downloader for NetCDF files from eReefs.

    Parameters:
    - base_url (str): The root URL containing the NetCDF files.
    - download_dir (str): Local directory to save downloaded files.
    - start_year/month/day: Start of the date range.
    - end_year/month/day: End of the date range.
    - file_prefix (str): The beginning of the filename, before the date.
    - file_suffix (str): Usually ".nc", ".nc4", or ".nc.gz".
    - date_format (str): Python datetime format matching the date portion of the filenames.
    - start_day/end_day (int, optional): Only needed for daily data (not monthly).
    - verbose (bool): If True, explains how the function works and prints progress.

    Example:
    download_netcdf(
        base_url="https://example.com/data/",
        download_dir="./downloads/",
        start_year=2020, start_month=1, start_day=1,
        end_year=2020, end_month=1, end_day=3,
        file_prefix="mydata_",
        date_format="%Y%m%d"
    )
    """
    if verbose:
        print("=" * 60)
        print("🌀 NetCDF Downloader")
        print("- Downloads NetCDF files based on a date pattern in the filename")
        print("- Supports both daily and monthly files")
        print("- Checks for file existence using HTTP HEAD before download")
        print("- Allows filename customization using prefix, suffix, and date format")
        print("=" * 60)
        print()

    os.makedirs(download_dir, exist_ok=True)
    start_day = start_day or 1
    end_day = end_day or 28  # default safe day

    start_date = datetime(start_year, start_month, start_day)
    end_date = datetime(end_year, end_month, end_day)

    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime(date_format)
        file_name = f"{file_prefix}{date_str}{file_suffix}"
        file_url = os.path.join(base_url, file_name)
        file_path = os.path.join(download_dir, file_name)

        if verbose:
            print(f"🔍 Checking: {file_url}")

        response = requests.head(file_url)
        if response.status_code == 200:
            if not os.path.exists(file_path):
                if verbose:
                    print(f"⬇️  Downloading: {file_name}")
                file_response = requests.get(file_url)
                with open(file_path, "wb") as f:
                    f.write(file_response.content)
                if verbose:
                    print(f"✅ Saved to: {file_path}")
            else:
                if verbose:
                    print(f"⚠️  Already exists: {file_path}")
        else:
            if verbose:
                print(f"❌ File not found (HTTP {response.status_code}): {file_url}")

        # Increment by day or month
        if "%d" in date_format:
            current_date += timedelta(days=1)
        else:
            if current_date.month == 12:
                current_date = datetime(current_date.year + 1, 1, 1)
            else:
                current_date = datetime(current_date.year, current_date.month + 1, 1)

    if verbose:
        print("\n✅ Download process completed.\n")


def download_netcdf_noaa(
    base_url,
    download_dir,
    start_year,
    start_month,
    end_year,
    end_month,
    file_prefix,
    file_suffix=".nc",
    date_format="%Y-%m-%d",
    start_day=None,
    end_day=None,
    verbose=True
):
    """
    Generic downloader for NetCDF files from NOAA.

    Parameters:
    - base_url (str): The root URL containing the NetCDF files.
    - download_dir (str): Local directory to save downloaded files.
    - start_year/month/day: Start of the date range.
    - end_year/month/day: End of the date range.
    - file_prefix (str): The beginning of the filename, before the date.
    - file_suffix (str): Usually ".nc", ".nc4", or ".nc.gz".
    - date_format (str): Python datetime format matching the date portion of the filenames.
    - start_day/end_day (int, optional): Only needed for daily data (not monthly).
    - verbose (bool): If True, explains how the function works and prints progress.

    Example:
    download_netcdf_noaa(
        base_url="https://www.star.nesdis.noaa.gov/pub/socd/mecb/crw/data/5km/v3.1_op/nc/v1.0/daily/dhw/",
        download_dir="./downloads/",
        start_year=2010, start_month=1, start_day=1,
        end_year=2010, end_month=12, end_day=31,
        file_prefix="coraltemp_v3.1_",
        date_format="%Y%m%d"
    )
    """
    if verbose:
        print("=" * 60)
        print("🌀 NOAA NetCDF Downloader")
        print("- Downloads NetCDF files from NOAA based on date patterns in filenames")
        print("- Supports daily files organized by year")
        print("- Checks for file existence using HTTP HEAD before downloading")
        print("- Allows filename customization using prefix, suffix, and date format")
        print("=" * 60)
        print()

    os.makedirs(download_dir, exist_ok=True)

    start_day = start_day or 1
    end_day = end_day or 31  # Default to end of the month if not provided

    start_date = datetime(start_year, start_month, start_day)
    end_date = datetime(end_year, end_month, end_day)

    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime(date_format)
        file_name = f"{file_prefix}{date_str}{file_suffix}"

        # Construct the URL, ensuring to correctly format the year part.
        # The directory structure includes a year subfolder (e.g., 2010, 2011)
        # So, we must construct the URL manually instead of using os.path.join().
        year_str = current_date.strftime("%Y")
        file_url = "/".join([base_url.rstrip("/"), year_str, file_name])
        file_path = os.path.join(download_dir, file_name)

        if verbose:
            print(f"🔍 Checking: {file_url}")

        # HTTP HEAD request to check if the file exists
        response = requests.head(file_url)
        if response.status_code == 200:
            if not os.path.exists(file_path):
                if verbose:
                    print(f"⬇️  Downloading: {file_name}")
                file_response = requests.get(file_url)
                with open(file_path, "wb") as f:
                    f.write(file_response.content)
                if verbose:
                    print(f"✅ Saved to: {file_path}")
            else:
                if verbose:
                    print(f"⚠️  Already exists: {file_path}")
        else:
            if verbose:
                print(f"❌ File not found (HTTP {response.status_code}): {file_url}")

        # Increment by day or month
        if "%d" in date_format:
            current_date += timedelta(days=1)
        else:
            if current_date.month == 12:
                current_date = datetime(current_date.year + 1, 1, 1)
            else:
                current_date = datetime(current_date.year, current_date.month + 1, 1)

    if verbose:
        print("\n✅ Download process completed.\n")


def download_netcdf_csiro(
    base_url,
    download_dir,
    start_year,
    start_month,
    end_year,
    end_month,
    file_prefix,
    file_suffix=".nc",
    date_format="%Y%m%d",
    start_day=None,
    end_day=None,
    verbose=True
):
    """
    Generic downloader for NetCDF files from CSIRO Wave Hindcast Data (Thredds server).

    Parameters:
    - base_url (str): The root URL containing the NetCDF files (e.g., base URL of CSIRO's Thredds server).
    - download_dir (str): Local directory to save downloaded files.
    - start_year/month/day: Start of the date range.
    - end_year/month/day: End of the date range.
    - file_prefix (str): The beginning of the filename, before the date.
    - file_suffix (str): Usually ".nc", ".nc4", or ".nc.gz".
    - date_format (str): Python datetime format matching the date portion of the filenames.
    - start_day/end_day (int, optional): Only needed for daily data (not monthly).
    - verbose (bool): If True, explains how the function works and prints progress.

    Example:
    download_netcdf_csiro(
        base_url="https://data-cbr.csiro.au//thredds/fileServer/catch_all/CMAR_CAWCR-Wave_archive/CAWCR_Wave_Hindcast_aggregate/gridded/",
        download_dir="./downloads/",
        start_year=1979, start_month=1, start_day=1,
        end_year=1994, end_month=12, end_day=31,
        file_prefix="ww3.aus_4m.",
        date_format="%Y%m%d"
    )
    """
    if verbose:
        print("=" * 60)
        print("🌀 CSIRO Wave Hindcast NetCDF Downloader")
        print("- Downloads NetCDF files from CSIRO's Wave Hindcast archive")
        print("- Checks for file existence using HTTP HEAD before download")
        print("- Allows filename customization using prefix, suffix, and date format")
        print("=" * 60)
        print()

    os.makedirs(download_dir, exist_ok=True)
    start_day = start_day or 1
    end_day = end_day or 28  # default safe day

    start_date = datetime(start_year, start_month, start_day)
    end_date = datetime(end_year, end_month, end_day)

    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime(date_format)
        file_name = f"{file_prefix}{date_str}{file_suffix}"
        file_url = os.path.join(base_url, file_name)
        file_path = os.path.join(download_dir, file_name)

        if verbose:
            print(f"🔍 Checking: {file_url}")

        response = requests.head(file_url)
        if response.status_code == 200:
            if not os.path.exists(file_path):
                if verbose:
                    print(f"⬇️  Downloading: {file_name}")
                file_response = requests.get(file_url)
                with open(file_path, "wb") as f:
                    f.write(file_response.content)
                if verbose:
                    print(f"✅ Saved to: {file_path}")
            else:
                if verbose:
                    print(f"⚠️  Already exists: {file_path}")
        else:
            if verbose:
                print(f"❌ File not found (HTTP {response.status_code}): {file_url}")

        # Increment by day or month
        if "%d" in date_format:
            current_date += timedelta(days=1)
        else:
            if current_date.month == 12:
                current_date = datetime(current_date.year + 1, 1, 1)
            else:
                current_date = datetime(current_date.year, current_date.month + 1, 1)

    if verbose:
        print("\n✅ Download process completed.\n")



