#!/bin/bash
# Downloads current DSN Now XML file and archives it

OUT_DIR="../../../data/raw"
ERROR_DIR="../../../data/errors"
URL="https://eyes.nasa.gov/dsn/data/dsn.xml"
SLEEP_DURATION=4

single_scrape () {
    today_archive="$(date --utc -Idate).zip"
    path_archive="$OUT_DIR/$today_archive"
    xml_file=$(echo "$(date --utc --iso-8601=seconds).xml" | tr : _)

    if curl -s -o "$xml_file" "$URL"; then
        # Attempt to add the XML file to the zip archive
        if zip -q -g "$path_archive" "$xml_file"; then
            rm "$xml_file"
        else
            echo "Failed to add $xml_file to $path_archive" >&2
            mkdir -p "$ERROR_DIR"
            # Move the XML file to the error directory
            mv "$xml_file" "$ERROR_DIR/"
        fi
    else
        echo "Failed to download XML file: $xml_file" >&2
        rm "$xml_file"
    fi
}

if [ ! -d "$OUT_DIR" ]; then
    echo "Creating output directory $OUT_DIR"
    mkdir -p "$OUT_DIR"
fi

main_loop () {
    while true; do
        single_scrape
        sleep "$SLEEP_DURATION"
    done
}

main_loop
