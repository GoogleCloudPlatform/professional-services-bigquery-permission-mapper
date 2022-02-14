# Copyright 2021 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Module for end-to-end tests."""

import csv
import json
import os
import shutil
from distutils import dir_util

import td2bq
from td2bq_mapper import td2bq_util

rootdir = td2bq_util.get_root_dir()
testdir = rootdir + "/tests"
tmpdir = testdir + "/temp_dir"


def ordered(obj):
    """Sort the input.

    Args:
      obj: an input object to sort

    Returns:
      The sorted input object if sorting is defined. Returns unchanged object
        otherwise.
    """
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj


def sort_permissions(map: list) -> list:
    """Sort BigQuery resulting permissions for comparison.

    Args:
      map: a list of permissions

    Returns:
      list: sorted list of permissions.
    """
    result = map[:]
    for i in range(len(map)):
        result[i][1] = set(result[i][1].split("\n"))
        for j in range(2, len(map[i])):
            result[i][j] = set(result[i][j].split(","))
    return result


def assert_file_content(target_dir: str, result_dir: str, files: list) -> bool:
    """Compares files with the same name in two directories.

    Args:
      target_dir: one folder name to go through
      result_dir: the other folder name to go through
      files: list of files in both dirs

    Returns:
      bool: True if all files with the same name contains the same information.
        False otherwise.
    """
    for f in files:
        with open(os.path.join(target_dir, f)) as f1, open(
            os.path.join(result_dir, f)
        ) as f2:
            _, ext = os.path.splitext(f)
            if str.lower(ext) == ".json":
                json1 = json.load(f1)
                json2 = json.load(f2)
                if ordered(json1) != ordered(json2):
                    return False
            elif str.lower(ext) == ".csv":
                if str.lower(f) == "td2bq_mapping.csv":
                    # account for grouped entities listed in different order
                    map1 = list(csv.reader(f1, delimiter=",", quotechar='"'))
                    map2 = list(csv.reader(f2, delimiter=",", quotechar='"'))
                    if len(map1) != len(map2) or map1[0] != map2[0]:
                        return False
                    map1 = sort_permissions(map1[1:])
                    map2 = sort_permissions(map2[1:])
                    if map1 != map2:
                        return False
                elif f1.read() != f2.read():
                    return False
            else:
                return False
    return True


def walk_dirs(target_dir: str) -> dict:
    """lists files and dirs in the given directory.

    Args:
      tqrget_dir: folder name to go through

    Returns:
      dict: contains files and directories in the given folder
    """
    strip = len(target_dir)
    targets = {}
    for path, dirs, files in os.walk(target_dir):
        key = path[strip:]
        # use slice assignment to skip the recurcive traversal of .dir
        dirs[:] = [d for d in dirs if not d[0] == "."]
        targets[key] = {
            "dirs": set(dirs),
            "files": set([f for f in files if not f[0] == "."]),
        }
    return targets


def generate_jsons(test_case: str):
    """Test JSON generation.

    Args:
      test_case: folder name containing the test data
    """
    # test dirs to compare with:
    source_dir = os.path.join(testdir, test_case, "input")
    target_dir = os.path.join(testdir, test_case, "output")

    # dirs generated during the test run:
    change_folder = os.path.join(tmpdir, "test_result")
    try:
        shutil.rmtree(change_folder)
    except FileNotFoundError:
        pass
    dir_util.copy_tree(source_dir, change_folder)

    # command line arguments to generate JSON:
    overwrite = False
    td_acl_file = os.path.join(source_dir, "td_permissions.csv")
    project_id = "acl-mapper-test"

    td2bq.generate_jsons(td_acl_file, change_folder, project_id, overwrite)

    assert os.path.isdir(os.path.join(change_folder, "json_generated")) is True

    targets = walk_dirs(target_dir)
    results = walk_dirs(change_folder)
    # assert that all files were generated
    for key in targets:
        try:
            assert key in results
            assert targets[key]["dirs"] == results[key]["dirs"]
            assert targets[key]["files"] == results[key]["files"]
            # assert the content of each file
            assert (
                assert_file_content(
                    target_dir + key, change_folder + key, targets[key]["files"]
                )
                is True
            )
        except AssertionError:
            print(
                f"Assertion fails with key = {key} \n "
                f"targets = {targets[key]} \n results = {results[key]}"
            )
            raise

    # assert that no other files were generated
    assert set(targets.keys()) == set(results.keys())
    shutil.rmtree(change_folder)


def test_generate_jsons_test6():
    """Test 6.

    Tests:
      valid permissions: {R}
      Renaming datasets and tables
    Args: None

    Returns:
      None
    """
    generate_jsons("test6")


def test_generate_jsons_test7():
    """Test 7.

    Tests:
      invalid permissions: {RS}, {I, R}
      valid permissions: {R}
      Renaming ivalid and valid TD roles to a single BQ role

    Args: None

    Returns:
      None
    """
    generate_jsons("test7")


def test_generate_jsons_test8():
    """Test 8.

    Tests:
        valid permissions: {R}, {R, I, U, D}
        Renaming two TD roles to a single BQ role

    Args: None

    Returns:
      None
    """
    generate_jsons("test6")


if __name__ == "__main__":
    test_generate_jsons_test7()
