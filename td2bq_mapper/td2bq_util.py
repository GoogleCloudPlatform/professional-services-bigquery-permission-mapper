# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Module for basic file and directory operations."""
import json
import logging
import os
import pickle

logger = logging.getLogger("td2bq")


def get_root_dir() -> str:
    """Returns full path to the main ACL Mapper module."""
    rootdir = os.path.dirname(os.path.abspath(__file__))
    return rootdir


def get_abs_path(path: str) -> str:
    """Convert local to the mapper path to the absolute path.

    Args:
      path (string): path

    Returns:
      dictionary: json map_file dictionary. None on error.
    """
    if os.path.isabs(path):
        return path
    return os.path.abspath(os.path.join(os.path.join(get_root_dir(), os.pardir), path))


def read_json_file(file_name: str) -> dict:
    """Read json file into python dictionary and error check.

    Args:
      file_name (string): file name where to read file from.

    Returns:
      dictionary: json map_file dictionary. None on error.
    """
    try:
        fd = open(file_name, "r")
        mapfile = json.load(fd)
        logger.info(f"Loaded json file: {file_name}")
        fd.close()
    except Exception:
        logger.exception("Error, could not load file: %s", file_name)
        return None
    return mapfile


def write_json_file(file_name: str, data: dict) -> bool:
    """Write out a dictionary into a json file and error check.

    Args:
      file_name (string): file name where to download map file.
      data: the dictionary to convert to JSON

    Returns:
      bool: True on success and False on failure
    """
    try:
        fd = open(file_name, "w")
        _ = json.dump(data, fd, indent=4)  # create mapfile
        logger.info("Wrote json file: %s", file_name)
        fd.close()
    except Exception:
        logger.exception("Error, could not write file on disk: %s", file_name)
        return False
    return True


def write_dict_file(file_name: str, indata: dict) -> bool:
    """Write out dictionary as a pickle file.

    Args:
      file_name (string): file name where to download map file.

    Returns:
      bool: True on success and False on failure
    """
    try:
        map2file = open(file_name, "wb")
        pickle.dump(indata, map2file)
        map2file.close()
    except Exception:
        logger.exception("Error, could not write file on disk: %s", file_name)
        return False
    return True


def read_dict_file(file_name: str) -> dict:
    """Read pickle file into python dictionary and error check.

    Args:
      file_name (string): file name where to read file from.

    Returns:
      dictionary: json map_file dictionary. None on error.
    """
    try:
        map2file = open(file_name, "rb")
        dict_data = pickle.load(map2file)
        map2file.close()
    except Exception:
        logger.exception("Error, could not load file: %s", file_name)
        return None
    return dict_data


def make_dirs(path: str, overwrite: bool, files=None) -> bool:
    """Create all missing directories in the path.

    Args:
      path(str): path to create
      overwrite(str): if True overwrite the existing files
      files(set): files to overwrite
    Returns:
      True if directories in the path were created
    """
    abs_path = os.path.abspath(path)
    if not overwrite and os.path.isdir(abs_path) and files:
        old_files = files.intersection(
            {os.path.join(abs_path, f) for f in os.listdir(abs_path)}
        )
        if old_files:
            logger.error(
                "Directory %s contains outputs from the previous "
                "ACL Mapper run. Remove files %s and rerun the mapper.",
                abs_path,
                old_files,
            )
            return False
    os.makedirs(abs_path, exist_ok=True)
    logger.debug("Created path %s", abs_path)
    return True
