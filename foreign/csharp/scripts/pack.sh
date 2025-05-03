#!/bin/bash

# Function to extract the version number from the commit message using regular expressions
function extract_version {
  local commit_message="$1"

  if [[ $commit_message =~ version-([0-9]+\.[0-9]+\.[0-9]+) ]]; then
    version=${BASH_REMATCH[1]}
    echo "$version"
  else
    echo "Version not found in the commit message."
    exit 1
  fi
}

commit_message=$(git log -1 --pretty=format:"%s")

version=$(extract_version "$commit_message")

echo "Extracted version: $version"

echo "Executing after success scripts on branch $GITHUB_REF_NAME"
echo "Triggering Nuget package build"

cd Iggy_SDK
dotnet pack -c release /p:PackageVersion=$version --no-restore -o .

echo "Uploading Iggy package to Nuget using branch $GITHUB_REF_NAME"

case "$GITHUB_REF_NAME" in
  "master")
    dotnet nuget push *.nupkg -k $NUGET_API_KEY -s https://api.nuget.org/v3/index.json
    echo "Published package succesfully!"
    ;;
  *)
    echo "Skipping NuGet package push as the branch is not master."
    ;;
esac


