import os
import time
import numpy as np
import pandas as pd
import laspy

from pcsfc.point_processor import compute_split_length, PointProcessor
from db import Postgres


class FileLoader:
    def __init__(self, name, parameters):
        self.path = parameters["path"]
        self.ratio = parameters["ratio"]
        self.tail_len = None

        self.meta = self.get_metadata(name, parameters["srid"])
        print(self.meta)

    def get_metadata(self, name, srid):
        # name, srid, point_count, head_len, tail_len, scale, offset, bbox
        with laspy.open(self.path) as f:
            point_count = f.header.point_count
            scales, offsets = f.header.scales, f.header.offsets
            bbox = [f.header.x_min, f.header.x_max, f.header.y_min, f.header.y_max, f.header.z_min, f.header.z_max]

            X_max = round((f.header.x_max - self.offsets[0]) / self.scales[0])
            Y_max = round((f.header.y_max - self.offsets[1]) / self.scales[1])
            head_len, self.tail_len = compute_split_length(X_max, Y_max, self.ratio)

        meta = [name, srid, point_count, head_len, self.tail_len, scales, offsets, bbox]
        return meta

    def preparation(self):
        processor = PointProcessor(self.path, self.tail_len)
        processor.execute()

    def loading(self, db_conf):
        start_time = time.time()
        db = Postgres(db_conf, self.name)
        db.connect()

        db.create_table()
        db.insert_metadata(self.meta)
        db.copy_points()

        load_time = time.time()
        print("-> Loading time:", round(load_time - start_time, 2))

        db.create_btree_index()
        db.disconnect()
        print("-> Close time:", round(time.time() - load_time, 2))


class DirLoader:
    def __init__(self, name, parameters):
        self.name = name
        self.paths = self.get_file_paths(parameters["path"])
        self.tail_len = None

        self.meta = self.get_metadata(parameters["srid"], parameters["ratio"])
        print("The number of files: ", len(self.paths))
        print(self.meta)

    def get_metadata(self, srid, ratio):
        # 1. Iterate each file, read the header and extract point cloud and bbox
        with laspy.open(self.paths[0]) as f:
            point_count = f.header.point_count
            scales, offsets = f.header.scales, f.header.offses
            x_min, y_min, z_min = f.header.x_min, f.header.y_min, f.header.z_min
            x_max, y_max, z_max = f.header.x_max, f.header.y_max, f.header.z_max

        for i in range(1, len(self.paths)):
            with laspy.open(self.paths[i]) as f:
                point_count += f.header.point_count
                x_min = min(x_min, f.header.x_min)
                x_max = max(x_max, f.header.x_max)
                y_min = min(y_min, f.header.y_min)
                y_max = max(y_max, f.header.y_max)
                z_min = min(z_min, f.header.z_min)
                z_max = max(z_max, f.header.z_max)
        bbox = [x_min, x_max, y_min, y_max, z_min, z_max]

        # 2. Based on the bbox of the whole point cloud, determine head_length and tail_length
        X_max = round((f.header.x_max - self.offsets[0]) / self.scales[0])
        Y_max = round((f.header.y_max - self.offsets[1]) / self.scales[1])
        head_len, tail_len = compute_split_length(X_max, Y_max, ratio)
        meta = [self.name, srid, point_count, head_len, tail_len, scales, offsets, bbox]
        return meta

    def run(self, db_conf):
        db = Postgres(db_conf, self.name)
        db.connect()

        db.create_table()
        db.insert_metadata(self.meta)

        load_time_count = 0
        for i in range(len(self.paths)):
            if i % 50 == 0:
                print(i, " is being processed.")

            # Preparation: Encode, split and group the Morton keys
            processor = PointProcessor(self.paths[i], self.meta[4])# tail_len
            processor.execute()

            # Import the data into the database
            load_time_1 = time.time()
            db = Postgres(db_conf, self.name)
            db.connect()
            if i == 0:
                db.create_table()
                db.insert_metadata(self.meta)

            db.copy_points()

            if i == (len(self.paths)-1):
                close_time_1 = time.time()
                db.create_btree_index()
                db.disconnect()
                close_time_count = time.time() - close_time_1

            else:
                db.disconnect()

            load_time_count += time.time() - load_time_1

        print("-> Load time:", round(load_time_count-(close_time_count), 2))
        print("-> Close time:", round(close_time_count, 2))


    def get_file_paths(self, dir_path):
        return [os.path.join(dir_path, file) for file in os.listdir(dir_path) if
                      os.path.isfile(os.path.join(dir_path, file))]
