import csv
import json


def write_csv(file_name, data, fieldnames):
    try:
        with open(file_name, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
    except IOError:
        print("write operation failed")


def transform_csv_json(file):
    print('DataTransformer: Transforming file: ' + file)
    f = open(file, 'r')

    # Get the headings
    fn = f.readlines(1)[0].split(',')

    reader = csv.DictReader(f, fieldnames=fn)
    # Convert to JSON
    out = json.dumps([row for row in reader])
    f.close()
    print(file)

    # Store the JSON in a file
    name = file.split('.')[0]
    key = file.split('.')[0] + ".json"
    name = key

    # opening and writing
    f = open(name, 'w+')
    f.write(out)
    f.close()
    print('Transform done')
    return name