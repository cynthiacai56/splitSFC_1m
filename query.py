import sys
import json
import time
import argparse

from pipeline.retrieve_data import Querier

def main():
    parser = argparse.ArgumentParser(description='Example of argparse usage.')
    parser.add_argument('--input', type=str, default="./scripts/query_20m.json", help='Input parameter json file path.')
    parser.add_argument('--password', type=str, default="123456", help='Input parameter json file path.')
    args = parser.parse_args()
    #jparams_path = "./scripts/query_20m_local.json"
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

    for key, value in jparams["queries"].items():
        start_time = time.time()
        query_name, source_name, mode, geometry = key, value["source_dataset"], value["mode"], value["geometry"]
        print(f"=== {mode} query {key} from {source_name} ===")

        try:
            pipeline = Querier(query_name, source_name, db_conf)
            pipeline.geometry_query(mode, geometry)

            if "maxz" in value:
                pipeline.maxz_query(value["maxz"])
            if "minz" in value:
                pipeline.minz_query(value["minz"])

            pipeline.disconnect()
        except Exception as e:
            print(f"An error occurred: {e}")

        print("-->%ss" % round(time.time() - start_time, 2))


if __name__ == '__main__':
    main()