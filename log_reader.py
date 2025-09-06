import csv
import re
import argparse
from collections import defaultdict

def main():
    # Set up command-line arguments
    parser = argparse.ArgumentParser(
        description="Process a compression log file and output a CSV summary."
    )
    parser.add_argument("log_file", help="Path to the compression log file")
    parser.add_argument(
        "--output_file",
        default="output.csv",
        help="Path to the output CSV file (default: output.csv)"
    )
    args = parser.parse_args()

    # Define all expected field names
    all_fields = [
        "RefID",
        "Pos",
        "Mapq",
        "Bin",
        "Flags",
        "NextRefID",
        "NextPos",
        "TemplateLength",
        "ReadName",
        "RawCigar",
        "RawSequence",
        "RawQual",
        "RawTags",
        "LName",
        "NCigar",
        "SequenceLength",
        "RawTagsLen",
        "RawSeqLen",
    ]

    # Dictionary to accumulate sizes per field
    field_data = defaultdict(lambda: {"compressed": 0, "uncompressed": 0})

    # Process the log file provided as an argument
    with open(args.log_file, "r") as file:
        for line in file:
            match = re.match(
                r"Field: (\w+), Uncompressed: (\d+), Compressed: (\d+)", line
            )
            if match:
                field, uncompressed, compressed = match.groups()
                field_data[field]["uncompressed"] += int(uncompressed)
                field_data[field]["compressed"] += int(compressed)

    # Write the accumulated data to CSV
    with open(args.output_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["field name", "compress size", "uncompressed size"])
        for field in all_fields:
            compressed = field_data[field]["compressed"]
            uncompressed = field_data[field]["uncompressed"]
            writer.writerow([field, compressed, uncompressed])

if __name__ == "__main__":
    main()
