import os
import calendar
from datetime import datetime, timedelta
import requests


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
    Downloader for eReefs NetCDF files from THREDDS fileServer.

    Example
    -------
    download_netcdf_ereefs(
        base_url="https://thredds.ereefs.aims.gov.au/thredds/fileServer/ereefs/gbr1_2.0/daily-daily",
        download_dir="./downloads",
        start_year=2017, start_month=8, start_day=27,
        end_year=2017, end_month=8, end_day=29,
        file_prefix="EREEFS_AIMS-CSIRO_gbr1_2.0_hydro_daily-daily-",
        date_format="%Y-%m-%d"
    )
    """

    if verbose:
        print("=" * 60)
        print("eReefs NetCDF Downloader")
        print("=" * 60)

    os.makedirs(download_dir, exist_ok=True)

    is_daily = "%d" in date_format

    if is_daily:
        start_day = start_day or 1
        end_day = end_day or calendar.monthrange(end_year, end_month)[1]

        start_date = datetime(start_year, start_month, start_day)
        end_date = datetime(end_year, end_month, end_day)

    else:
        start_date = datetime(start_year, start_month, 1)
        end_date = datetime(end_year, end_month, 1)

    current_date = start_date

    while current_date <= end_date:

        date_str = current_date.strftime(date_format)
        file_name = f"{file_prefix}{date_str}{file_suffix}"

        file_url = f"{base_url.rstrip('/')}/{file_name}"
        file_path = os.path.join(download_dir, file_name)

        if verbose:
            print(f"Checking: {file_url}")

        try:

            response = requests.head(file_url, allow_redirects=True, timeout=30)

            if response.status_code != 200:
                if verbose:
                    print(f"File not found (HTTP {response.status_code})")
                raise ValueError

            if os.path.exists(file_path):
                if verbose:
                    print("Already downloaded.")
                raise ValueError

            r = requests.get(file_url, stream=True, timeout=120)
            r.raise_for_status()

            content_type = r.headers.get("Content-Type", "")

            if "html" in content_type.lower():
                if verbose:
                    print("Server returned HTML instead of NetCDF — skipping.")
                raise ValueError

            with open(file_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            # Validate NetCDF signature
            with open(file_path, "rb") as f:
                sig = f.read(4)

            if not (sig.startswith(b"CDF") or sig.startswith(b"\x89HDF")):
                os.remove(file_path)
                if verbose:
                    print("Downloaded file was not NetCDF. Deleted.")
            else:
                if verbose:
                    print(f"Saved: {file_path}")

        except Exception:
            pass

        if is_daily:
            current_date += timedelta(days=1)
        else:
            if current_date.month == 12:
                current_date = datetime(current_date.year + 1, 1, 1)
            else:
                current_date = datetime(current_date.year, current_date.month + 1, 1)

    if verbose:
        print("\nDownload completed.\n")


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
    - base_url (str): Root URL containing the NetCDF files.
    - download_dir (str): Local directory to save downloaded files.
    - start_year, start_month, start_day: Start of the date range.
    - end_year, end_month, end_day: End of the date range.
    - file_prefix (str): Beginning of the filename, before the date.
    - file_suffix (str): Usually ".nc", ".nc4", or ".nc.gz".
    - date_format (str): Datetime format matching the date part of the filenames.
    - start_day/end_day (int, optional): Needed for daily data. Ignored for monthly data.
    - verbose (bool): If True, prints progress.

    Example:
    download_netcdf_noaa(
        base_url="https://www.star.nesdis.noaa.gov/pub/socd/mecb/crw/data/5km/v3.1_op/nc/v1.0/daily/dhw/",
        download_dir="./downloads/",
        start_year=2023, start_month=1, start_day=15,
        end_year=2023, end_month=2, end_day=10,
        file_prefix="ct5km_dhw_v3.1_",
        date_format="%Y%m%d"
    )
    """

    if verbose:
        print("=" * 60)
        print("NOAA NetCDF Downloader")
        print("- Downloads NetCDF files from NOAA based on date patterns in filenames")
        print("- Supports daily or monthly files")
        print("- Checks file existence before downloading")
        print("=" * 60)
        print()

    os.makedirs(download_dir, exist_ok=True)

    is_daily = "%d" in date_format

    if is_daily:
        if start_day is None:
            start_day = 1
        if end_day is None:
            end_day = calendar.monthrange(end_year, end_month)[1]

        start_date = datetime(start_year, start_month, start_day)
        end_date = datetime(end_year, end_month, end_day)
    else:
        # For monthly data, use the first day of each month internally
        start_date = datetime(start_year, start_month, 1)
        end_date = datetime(end_year, end_month, 1)

    current_date = start_date

    while current_date <= end_date:
        date_str = current_date.strftime(date_format)
        file_name = f"{file_prefix}{date_str}{file_suffix}"

        year_str = current_date.strftime("%Y")
        file_url = f"{base_url.rstrip('/')}/{year_str}/{file_name}"
        file_path = os.path.join(download_dir, file_name)

        if verbose:
            print(f"Checking: {file_url}")

        try:
            response = requests.head(file_url, allow_redirects=True, timeout=30)

            if response.status_code == 200:
                if not os.path.exists(file_path):
                    if verbose:
                        print(f"Downloading: {file_name}")

                    file_response = requests.get(file_url, stream=True, timeout=60)
                    file_response.raise_for_status()

                    with open(file_path, "wb") as f:
                        for chunk in file_response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)

                    if verbose:
                        print(f"Saved to: {file_path}")
                else:
                    if verbose:
                        print(f"Already exists: {file_path}")
            else:
                if verbose:
                    print(f"File not found (HTTP {response.status_code}): {file_url}")

        except requests.RequestException as e:
            if verbose:
                print(f"Request failed for {file_url}")
                print(f"Error: {e}")

        # Increment date
        if is_daily:
            current_date += timedelta(days=1)
        else:
            if current_date.month == 12:
                current_date = datetime(current_date.year + 1, 1, 1)
            else:
                current_date = datetime(current_date.year, current_date.month + 1, 1)

    if verbose:
        print("\nDownload process completed.\n")


def download_netcdf_csiro(
    base_url,
    download_dir,
    start_year,
    start_month,
    end_year,
    end_month,
    file_prefix,
    file_suffix=".nc",
    date_format="%Y%m",
    verbose=True
):
    """
    Downloader for monthly CSIRO Wave Hindcast NetCDF files from a THREDDS fileServer.

    Example:
    download_netcdf_csiro(
        base_url="https://data-cbr.csiro.au/thredds/fileServer/catch_all/CMAR_CAWCR-Wave_archive/CAWCR_Wave_Hindcast_1979-2010/gridded",
        download_dir="./downloads",
        start_year=2010,
        start_month=1,
        end_year=2010,
        end_month=3,
        file_prefix="ww3.pac_4m.",
        date_format="%Y%m"
    )
    """

    if verbose:
        print("=" * 60)
        print("CSIRO Wave Hindcast NetCDF Downloader")
        print("- Downloads monthly NetCDF files from the CSIRO THREDDS fileServer")
        print("- Builds filenames from prefix + YYYYMM + suffix")
        print("=" * 60)
        print()

    os.makedirs(download_dir, exist_ok=True)

    current_date = datetime(start_year, start_month, 1)
    end_date = datetime(end_year, end_month, 1)

    while current_date <= end_date:
        date_str = current_date.strftime(date_format)
        file_name = f"{file_prefix}{date_str}{file_suffix}"
        file_url = f"{base_url.rstrip('/')}/{file_name}"
        file_path = os.path.join(download_dir, file_name)

        if verbose:
            print(f"Checking: {file_url}")

        try:
            response = requests.head(file_url, allow_redirects=True, timeout=30)

            if response.status_code == 200:
                if not os.path.exists(file_path):
                    if verbose:
                        print(f"Downloading: {file_name}")

                    file_response = requests.get(file_url, stream=True, timeout=120)
                    file_response.raise_for_status()

                    with open(file_path, "wb") as f:
                        for chunk in file_response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)

                    if verbose:
                        print(f"Saved to: {file_path}")
                else:
                    if verbose:
                        print(f"Already exists: {file_path}")
            else:
                if verbose:
                    print(f"File not found (HTTP {response.status_code}): {file_url}")

        except requests.RequestException as e:
            if verbose:
                print(f"Request failed for: {file_url}")
                print(f"Error: {e}")

        # Move to next month
        if current_date.month == 12:
            current_date = datetime(current_date.year + 1, 1, 1)
        else:
            current_date = datetime(current_date.year, current_date.month + 1, 1)

    if verbose:
        print("\nDownload process completed.\n")