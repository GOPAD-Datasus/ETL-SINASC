from etl import extract, transform
import json


if __name__ == '__main__':
    with open('parameters/params.json') as json_file:
        params = json.load(json_file)

    extract(params)
    transform(params)
    # load()