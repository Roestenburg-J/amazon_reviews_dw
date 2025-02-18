import os
import json


def write_failed_rows(file_path, failed_rows):

    # If file does not exist, create
    if not os.path.exists(file_path):
        with open(file_path, "w", encoding="utf-8") as jsonf:
            jsonf.write("[\n")

    # Check if file is empty, if empty create json array and write
    # If not empty, remove array syntax, add rows, close array syntax
    with open(file_path, "r+", encoding="utf-8") as jsonf:
        jsonf.seek(0, os.SEEK_END)
        file_size = jsonf.tell()

        if file_size > 2:
            jsonf.seek(file_size - 2)
            jsonf.truncate()
            jsonf.write(",\n")
        else:
            jsonf.seek(0, os.SEEK_END)

        for i, row in enumerate(failed_rows):
            jsonf.write(json.dumps(row, indent=4))
            if i < len(failed_rows) - 1:
                jsonf.write(",\n")
            else:
                jsonf.write("\n")

        jsonf.write("]")
