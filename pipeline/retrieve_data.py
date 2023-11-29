import numpy as np
import pandas as pd
import laspy
import time

from shapely.wkt import loads
from psycopg2 import connect, Error, extras

from pcsfc.decoder import DecodeMorton2DX, DecodeMorton2DY
from pcsfc.range_search import morton_range


class Querier:
    def __init__(self, query_name, source_name, db_conf):
        self.head_len = 28
        self.tail_len = 24
        self.scales = [0.01, 0.01, 0.01]
        self.offsets = [0, 400000, 0]

        self.source_table = "point_" + source_name
        self.name = query_name

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


    def geometry_query(self, mode, geometry):
        if mode == "bbox":
            self.bbox_query(geometry)
        elif mode == "circle":
            self.circle_query(geometry)
        elif mode == "polygon":
            self.polygon_query(geometry)
        elif mode == "nn":
            print("nn search is not developed yet.")

    def bbox_query(self, bbox):
        start_time = time.time()
        self.range_search(bbox)
        print("-> Filter step time:", round(time.time() - start_time, 2))

    def circle_query(self, geometry):
        start_time = time.time()
        # 1. Compute bounding box
        center_x, center_y, radius = geometry[0][0], geometry[0][1], geometry[1]
        x_min, x_max = center_x - radius, center_x + radius
        y_min, y_max = center_y - radius, center_y + radius
        bbox = [x_min, x_max, y_min, y_max]

        # 2. Range search based on bounding box and create table as intermediate result
        self.range_search(bbox)
        filter_time = time.time()
        print("-> Filter step time:", round(filter_time - start_time, 2))

        # 3. Use PostGIS function to query the points inside the circle, create table as result
        circle_query = f"""
            DELETE FROM {self.name}
            WHERE NOT ST_DWithin(point, ST_MakePoint({center_x}, {center_y}), {radius});
        """
        self.cursor.execute(circle_query)
        self.connection.commit()
        print(f"Circle search is updated in {self.name}.")
        print("-> Refinement (circle) step time:", round(time.time() - filter_time, 2))


    def polygon_query(self, wkt_string):
        start_time = time.time()
        # 1. Compute bounding box
        polygon = loads(wkt_string)
        exterior_coords = list(polygon.exterior.coords)
        x = [pt[0] for pt in exterior_coords]
        y = [pt[1] for pt in exterior_coords]
        bbox = [min(x), max(x), min(y), max(y)]

        # 2. Range search based on bounding box and create table as intermediate result
        self.range_search(bbox)
        filter_time = time.time()
        print("-> Filter step time:", round(filter_time - start_time, 2))

        # 3. Use PostGIS function to query the points inside the circle, create table as result
        polygon_query = f"""
            DELETE FROM {self.name}
            WHERE NOT ST_Within(point, ST_GeomFromText('{wkt_string}'))
        """
        self.cursor.execute(polygon_query)
        self.connection.commit()
        print(f"Polygon search is updated in {self.name}.")
        print("-> Refinement (polygon) step time:", round(time.time() - filter_time, 2))

    def maxz_query(self, maxz):
        start_time = time.time()
        z_query = f"""
            DELETE FROM {self.name}
            WHERE ST_Z(point) > {maxz}
        """
        self.cursor.execute(z_query)
        self.connection.commit()
        print(f"Max height search is updated in {self.name}.")
        print("-> Refinement (max_z) step time:", round(time.time() - start_time, 2))

    def minz_query(self, minz):
        start_time = time.time()
        z_query = f"""
            DELETE FROM {self.name}
            WHERE ST_Z(point) < {minz}
        """
        self.cursor.execute(z_query)
        self.connection.commit()
        print(f"Min height search is updated in {self.name} successfully.")
        print("-> Refinement (min_z) step time:", round(time.time() - start_time, 2))

    def range_search(self, bbox):
        # 0. Scale and shift the bounding box ,
        x_scale, y_scale = self.scales[0], self.scales[1]
        x_offset, y_offset = self.offsets[0], self.offsets[1]
        x_min, x_max = bbox[0] * x_scale + x_offset, bbox[1] * x_scale + x_offset
        y_min, y_max = bbox[2] * x_scale + x_offset, bbox[3] * x_scale + x_offset
        bbox = [x_min, x_max, y_min, y_max]

        # 1. Find the fully containing and overlapping heads
        head_ranges, head_overlaps = morton_range(bbox, 0, self.head_len, self.tail_len)

        # 2. Take these heads out of the database
        ## 2.1 Range query
        # Create a range table and insert data
        self.cursor.execute('DROP TABLE IF EXISTS RangeTable')
        self.cursor.execute('''CREATE TEMP TABLE RangeTable (range_start INT, range_end INT)''')
        self.cursor.executemany('INSERT INTO RangeTable (range_start, range_end) VALUES (%s, %s)', head_ranges)

        self.cursor.execute(f'''
            SELECT * FROM {self.source_table} 
            WHERE EXISTS (
                SELECT 1 FROM RangeTable 
                WHERE {self.source_table}.sfc_head BETWEEN RangeTable.range_start AND RangeTable.range_end
            )
        ''')
        res1 = self.cursor.fetchall() # data type: a list of tuple ?


        ## 2.2 Overlaps Query
        self.cursor.execute(f'''SELECT * FROM {self.source_table} WHERE sfc_head = ANY(%s)''', (head_overlaps,))
        res2 = self.cursor.fetchall()

        # 3. Unpack the point block and decode
        points_within_bbox = []
        for (sfc_head, sfc_tail, z) in res1:
            for i in range(len(sfc_tail)):
                sfc_key = sfc_head << self.tail_len | sfc_tail[i]
                x, y = DecodeMorton2DX(sfc_key)*x_scale+x_offset, DecodeMorton2DY(sfc_key)*y_scale+y_offset
                points_within_bbox.append([x, y, z[i]])

        for (sfc_head, sfc_tail, z) in res2:  # Each group
            # Check which tails of this head in within the ranges
            tail_rgs, tail_ols = morton_range(bbox, sfc_head, self.tail_len, 0)
            # Unpack the tails
            for i in range(len(sfc_tail)):  # Each point
                # Check if the tail in within the ranges
                check_in_range = any(start <= sfc_tail[i] <= end for start, end in tail_rgs)
                if check_in_range == 1:
                    sfc_key = sfc_head << self.tail_len | sfc_tail[i]
                    x, y = DecodeMorton2DX(sfc_key)*x_scale+x_offset, DecodeMorton2DY(sfc_key)*y_scale+y_offset
                    points_within_bbox.append([x, y, z[i]])

        # 4. Create results as a table
        self.cursor.execute(f"CREATE TABLE {self.name} (point geometry(PointZ));")
        insert_sql = f"INSERT INTO {self.name} VALUES (ST_MakePoint(%s, %s, %s));"
        for point in points_within_bbox:
            self.cursor.execute(insert_sql, point)
        self.connection.commit()
        print(f"Points (original values) within the bounding box are inserted into the table {self.name}.")

    def disconnect(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
            self.connection = None
            self.cursor = None
