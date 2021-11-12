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

import json
import logging
import shutil
from typing import Optional

import requests

from .td2bq_util import get_root_dir, read_json_file

logger = logging.getLogger("td2bq")


class Td2BqArcMap:
    """
    Td2BqArcMap class.  used to instantiate and store td2bq_permissions_map
    """

    # init method or constructor
    def __init__(self) -> None:
        """
        Td2BqArcMap constructor. Initialize class variables and start
        GCP storage client
        Args: None

        Returns:
            None
        """

        # default location for bucket and CSV object file.
        self.bucket_name = "td2bq-mapper-storage"
        self.object_name = "td2bq_permissions_map.json"
        self.td2bq_arc_map = None
        self.local_map_file = get_root_dir() + "/data/td2bq_arc_map.json"
        self.gcs_map_file = "/tmp/gcs_temp_map.json"

    def get_map_file_from_gcs(self, file_name: str) -> Optional[bool]:
        """
        Download the Permission map file.
        Args:
            file_name (string): file name where to download map file.

        Returns:
            bool: True if success None if failure.
        """
        arc_map_file_url = (
            "https://storage.googleapis.com/"
            + self.bucket_name
            + "/"
            + self.object_name
        )

        r = requests.get(arc_map_file_url, stream=True)

        if not r.ok:
            logger.info(f"Sorry, error downloading file: {arc_map_file_url}")
            return None

        try:
            r.raise_for_status()
        except Exception as error:
            logger.error(f"Sorry, error downloading to file {file_name} !", error)
            return None

        try:
            with open(file_name, "wb") as fd:
                # fd.write(r.content)
                for chunk in r.iter_content(chunk_size=10240):
                    # writing one chunk at a time to file
                    if chunk:
                        fd.write(chunk)
        except Exception as error:
            logger.error(f"Sorry, error writing to file {file_name} !", error)
            return None

        logger.info("Downloaded map file: ", file_name)
        return True

    def is_newer_version(self, other_map_file: dict) -> Optional[bool]:
        """
        Check if map file provided is newer than the local one
        Args:
            other_map_file (dictionary): map file json dictionary

        Returns:
            bool: True if other file is a new version. False if not.
                Returns None on error.
        """
        try:
            fd = open(self.local_map_file, "r")
            local_mapfile = json.load(fd)
            local_map_version = local_mapfile["version"]
            fd.close()
        except Exception as error:
            logger.error(f"Error, Could not load local mapfile: {self.local_map_file}")
            logger.error(error)
            return None

        other_map_version = other_map_file["version"]

        if other_map_version > local_map_version:
            return True
        else:
            return False

    def print_map(self, td2bq_arc_map: dict) -> None:
        """
        Print full td2bq permissions map
        Args:
            td2bq_arc_map(dictionary): map as a dictionary

        Returns:
            None: prints the full permissions map to stdout
        """
        header1 = "Access Right Code"
        header2 = "Description"
        header3 = "BigQuery Permission(s)"
        logger.info(f"{header1:17} | {header2:30} | {header3}")
        divider = "-".ljust(100, "-")
        logger.info(divider)

        del td2bq_arc_map["version"]
        for key, value in td2bq_arc_map.items():
            logger.info(
                f"{key:17} | {value['description']:30} | {value['bq_permissions']}"
            )

    def get_td2bq_arc_map(self, map_file_path: str = None) -> Optional[dict]:
        """
        Get td2bq full permissions map
        Args:
            map_file_path(string): path to mapfile

        Returns:
            dictionary: td2bq full permissions map
        """
        if map_file_path:
            self.update_local_mapfile(map_file_path)
        else:
            self.update_local_mapfile()

        mapfile = read_json_file(self.local_map_file)
        if mapfile is None:
            return None
        return mapfile

    def update_local_mapfile(self, map_file_path: str = None) -> bool:
        """
        Check and update local map file if needed.
        Args:
            None or map_file(String): Location of mapfile to use

        Returns:
            boolean: True if local file was updated. False otherwise.
        """
        if map_file_path:
            gcs_map_file = map_file_path
        else:
            gcs_map_file = self.gcs_map_file
            if self.get_map_file_from_gcs(gcs_map_file) is None:
                # something went wrong with getting file from GCS. Use local copy
                return False

        mapfile = read_json_file(gcs_map_file)
        if mapfile is None:
            # something is wrong with file we got. Use local copy.
            return False

        # check if gcs file is newer version
        if self.is_newer_version(mapfile):
            # copy mapfile and replace existing copy.
            try:
                shutil.copy(self.gcs_map_file, self.local_map_file)
                logger.info("A newer version of the map file exits online. Updating.")
            except Exception as error:
                logger.error("Error: Could not update local mapfile", error)
                return False
        else:
            # local file is same version or newer. No need to update
            logger.info("Using existing local copy of mapfile. ")
            logger.info(f"It is the same or a newer version than: {gcs_map_file}")
            return False

        return True
