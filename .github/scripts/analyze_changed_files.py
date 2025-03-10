#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import argparse
import json
import sys
import re
import os
import pprint


def load_config(config_file):
    """Load the configuration from a JSON file."""
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading config file: {e}")
        sys.exit(1)


def categorize_files(files, config):
    """Categorize files based on the configuration."""
    results = {}
    matched_files = {}

    for category, patterns in config.items():
        print(f"Checking files for category: {category}")
        results[category] = False
        matched_files[category] = []
        for file in files:
            print(f"  Checking {file}")
            for pattern in patterns:
                if re.match(f"^{pattern}$", file) is not None:
                    print(f"    Matched pattern: {pattern}")
                    results[category] = True
                    matched_files[category].append(file)
                    break

    return results, matched_files


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description=
        'Analyze changed files and categorize them based on patterns from a config file.\
        Outputs environment variables using GITHUB_ENV and GITHUB_OUTPUT for GitHub Actions to indicate\
        which categories have changed files. Environment variables are in the format:\
        CATEGORY_FILES_CHANGED=true and CATEGORY_FILES=file1.py\\nfile2.py\
        where CATEGORY is the name of the category in uppercase from the config file.'
    )

    parser.add_argument(
        'changed_files',
        metavar='changed_files',
        default=None,
        help='String with list of file changed in the PR or commit')

    parser.add_argument(
        'config_file',
        default=None,
        help='Path to the JSON configuration file with file patterns and groups'
    )

    parser.add_argument('-v',
                        '--verbose',
                        action='store_true',
                        default=False,
                        help='Enable verbose output for debugging')

    return parser.parse_args()


def main():
    """Main function."""
    args = parse_arguments()

    changed_files = args.changed_files.splitlines()
    config_file = args.config_file
    verbose = args.verbose
    config = load_config(config_file)

    if verbose:
        print("Changed files:")
        for file in changed_files:
            print(f"  {file}")
        print("\nConfig file:", config_file)
        pprint.pp(config)
        print("")

    # Categorize files
    results, matched_files = categorize_files(changed_files, config)

    # Set GitHub environment variables
    github_env = os.environ.get('GITHUB_ENV')
    github_output = os.environ.get('GITHUB_OUTPUT')

    # Output results
    print("\nChanged Files Analysis results:")

    for category, has_changes in results.items():
        category_upper = category.upper()
        print(f"  {category_upper}_FILES_CHANGED={str(has_changes).lower()}")

        if github_env:
            with open(github_env, 'a') as f:
                f.write(
                    f"{category_upper}_FILES_CHANGED={str(has_changes).lower()}\n"
                )

                if has_changes:
                    f.write(f"{category_upper}_FILES<<EOF\n")
                    for file in matched_files[category]:
                        f.write(f"{file}\n")
                    f.write("EOF\n")

        if github_output:
            with open(github_output, 'a') as f:
                f.write(
                    f"{category_upper}_FILES_CHANGED={str(has_changes).lower()}\n"
                )

                if has_changes:
                    f.write(f"{category_upper}_FILES<<EOF\n")
                    for file in matched_files[category]:
                        f.write(f"{file}\n")
                    f.write("EOF\n")

        # Print matched files for debugging
        if has_changes:
            for file in matched_files[category]:
                print(f"    {file}")
        else:
            print(f"Changed {category} files: None")


if __name__ == "__main__":
    main()
