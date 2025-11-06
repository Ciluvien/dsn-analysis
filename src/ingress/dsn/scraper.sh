#!/bin/bash
# Downloads current DSN Now XML file and archives it

OUT_DIR="../../../data/raw/"
ERROR_DIR="../../../data/errors/"
URL="https://eyes.nasa.gov/dsn/data/dsn.xml"
SLEEP_DURATION=4

scrape () {
    while true; do
        today_archive="$(date -Idate).zip"
        path_archive="$OUT_DIR$today_archive"
        xml_file="$(date +%s).xml"

        if curl -s -o "$xml_file" "$URL"; then
            # Attempt to add the XML file to the zip archive
            if zip -g "$path_archive" "$xml_file"; then
                rm "$xml_file"
            else
                echo "Failed to add $xml_file to $path_archive"
                mkdir -p "$ERROR_DIR"
                # Move the XML file to the error directory
                mv "$xml_file" "$ERROR_DIR"
            fi
        else
            echo "Failed to download XML file: $xml_file"
        fi

        sleep $SLEEP_DURATION
    done
}


if [ ! -d "$OUT_DIR" ]; then
    echo "Creating output directory $OUT_DIR"
    mkdir -p "$OUT_DIR"
fi

while true; do
    scrape
    echo "Scraper encountered an error. Restarting."
    sleep $SLEEP_DURATION
done
