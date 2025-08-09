import yaml

from etl import extract, transform, load


if __name__ == '__main__':
    with (open('input.yaml') as f):
        params = yaml.safe_load(f)['info']
        prefix = params['prefix']

    extract(prefix)
    transform()
    load()
