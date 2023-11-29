import sys
import json
import time
import argparse
from pipeline.import_data import FileLoader, DirLoader


def main():
    parser = argparse.ArgumentParser(description='Example of argparse usage.')
    parser.add_argument('--input', type=str, default="./scripts/import.json", help='Input parameter json file path.')
    parser.add_argument('--password', type=str, default="123456", help='Input parameter json file path.')
    args = parser.parse_args()
    jparams_path = "scripts/import_folder.json"
    jparams_path = args.input

    try:
        with open(jparams_path, 'r') as f:
            jparams = json.load(f)
    except FileNotFoundError:
        print("ERROR: File not found.")
    except json.JSONDecodeError as e:
        print(f"ERROR: JSON decoding error: {e}")
        sys.exit()

    db_conf = jparams["config"]
    db_conf["password"] = args.password

    for key, value in jparams["imports"].items():
        print(f"=== Import {key} into PostgreSQL===") # key is name
        start_time = time.time()

        try:
            if value["mode"] == "file":
                pipeline = FileLoader(key, value)
                pipeline.preparation()
                print("-> Initial time:", round(time.time() - start_time, 2))
                pipeline.loading(db_conf)

            elif value["mode"] == "dir":
                pipeline = DirLoader(key, value)
                pipeline.run(db_conf)

        except Exception as e:
            print(f"An error occurred: {e}")

        print("-> Total time: ", round(time.time() - start_time, 2))


if __name__ == '__main__':
    main()