#!/bin/bash

# Set version and base URL
DUCKDB_VERSION="v1.2.0"
BASE_URL="https://github.com/duckdb/duckdb/releases/download/${DUCKDB_VERSION}"

# Detect OS
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    ZIP_FILE="duckdb_cli-windows-amd64.zip"
    BINARY_NAME="duckdb.exe"
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    ZIP_FILE="duckdb_cli-linux-amd64.zip"
    BINARY_NAME="duckdb"
else
    echo "Unsupported operating system: $OSTYPE"
    exit 1
fi

# Download the zip file
echo "Downloading DuckDB for ${OSTYPE}..."
if ! curl --fail --location --progress-bar --output "${ZIP_FILE}" "${BASE_URL}/${ZIP_FILE}"; then
    echo "Download failed!"
    exit 1
fi

# Extract the binary
echo "Extracting DuckDB binary..."
if ! unzip -o "${ZIP_FILE}"; then
    echo "Extraction failed!"
    rm -f "${ZIP_FILE}"
    exit 1
fi

# Remove the zip file
if rm -f "${ZIP_FILE}"; then
    echo "Cleaned up zip file"
else
    echo "Failed to clean up zip file"
    exit 1
fi

# Verify binary exists
if [ -f "${BINARY_NAME}" ]; then
    echo "DuckDB installation successful!"
    exit 0
else
    echo "Binary not found after extraction!"
    exit 1
fi
