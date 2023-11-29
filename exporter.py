import sys
import json
import time
import argparse
import numpy as np
import laspy
from psycopg2 import connect, Error

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
        print(f"=== Convert table {key} to LAS file ===")

        pg2las = Pg2Las(db_conf, key)

        print(f"File {key}.las is created. ")
        print("-->%ss" % round(time.time() - start_time, 2))


class Pg2Las:
    def __init__(self, db_conf, table_name):
        """
        The schema of the table: one column, and the data type is Geomtry(PointZ).
        Args:
            db_conf:
            table_name:
        """
        self.table_name = table_name
        self.connection = None
        self.cursor = None

        self.connect_to_db(db_conf)
        self.read_data_from_pg()
        self.disconnect()

    def connect_to_db(self, db_conf):
        try:
            self.connection = connect(
                dbname=db_conf['dbname'],
                user=db_conf['user'],
                password=db_conf['password'],
                host=db_conf['host'],
                port=db_conf['port']
            )

            self.cursor = self.connection.cursor()
        except Error as e:
            print("Error: Unable to connect to the database.")
            print(e)

    def read_data_from_pg(self):
        self.cursor.execute(f"SELECT ST_X(point), ST_Y(point), ST_Z(point) FROM {self.table_name};")
        res = self.cursor.fetchall()  # a list of tuples
        my_data = np.array(res)
        self.write_las_file(my_data)

    def write_las_file(self, my_data, filename="query_results.las"):
        filename = f"{self.table_name}.las"
        header = laspy.LasHeader(point_format=3, version="1.2")
        header.offsets = np.array([0, 0, 0])
        header.scales = np.array([0.1, 0.1, 0.1])

        # 3. Create a LasWriter and a point record, then write it
        with laspy.open(filename, mode="w", header=header) as writer:
            point_record = laspy.ScaleAwarePointRecord.zeros(my_data.shape[0], header=header)
            point_record.x = my_data[:, 0]
            point_record.y = my_data[:, 1]
            point_record.z = my_data[:, 2]

            writer.write_points(point_record)

    def disconnect(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
            self.connection = None
            self.cursor = None

if __name__ == '__main__':
    main()