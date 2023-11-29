from psycopg2 import connect, Error, extras


class Postgres:
    def __init__(self, db_conf, name):
        self.db_conf = db_conf
        self.connection = None
        self.cursor = None

        self.meta_table = "metadata_" + name
        self.point_table = "point_" + name
        self.btree_index = "btree_idx_" + name

    def connect(self):
        try:
            self.connection = connect(
                dbname=self.db_conf["dbname"],
                user=self.db_conf["user"],
                password=self.db_conf["password"],
                host=self.db_conf["host"],
                port=self.db_conf["port"]
            )
            self.cursor = self.connection.cursor()
        except Error as e:
            print("Error: Unable to connect to the database.")
            print(e)

    def disconnect(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
            self.connection = None
            self.cursor = None

    def create_table(self, name="default"):
        if not self.connection:
            print("Error: Database connection is not established.")
            return

        create_table_sql = f"""
            CREATE EXTENSION IF NOT EXISTS postgis;
            CREATE TABLE IF NOT EXISTS {self.meta_table} (
                name TEXT,
                srid INT,
                point_count BIGINT,
                head_length INT,
                tail_length INT,
                scales DOUBLE PRECISION[],
                offsets DOUBLE PRECISION[],
                bbox DOUBLE PRECISION[]
            );        
            CREATE TABLE IF NOT EXISTS {self.point_table} (
                sfc_head INT,
                sfc_tail INT[],
                z DOUBLE PRECISION[]
            );
            """
        try:
            self.cursor.execute(create_table_sql)
            self.connection.commit()
        except Error as e:
            print("Error: Unable to create table")
            print(e)
            self.connection.rollback()

    def execute_sql(self, sql, data=None):
        if not self.connection:
            print("Error: Database connection is not established.")
            return
        try:
            if data:
                self.cursor.execute(sql, data)
            else:
                self.cursor.execute(sql)
            self.connection.commit()
        except Error as e:
            print(f"Error: Unable to execute query: {sql}")
            print(e)
            self.connection.rollback()

    def insert_metadata(self, data):
        if not self.connection:
            print("Error: Database connection is not established.")
            return

        try:
            self.cursor.execute(f"INSERT INTO {self.meta_table} VALUES (%s, %s, %s, %s, %s, %s, %s, %s);", data)
            self.connection.commit()
        except Error as e:
            print(f"Error: Unable to insert metadata.")
            print(e)
            self.connection.rollback()

    def copy_points(self, file="pc_record.csv"):
        if not self.connection:
            print("Error: Database connection is not established.")
            return

        with open(file, 'r') as f:
            try:
                self.cursor.copy_expert(sql=f"COPY {self.point_table} FROM stdin WITH CSV HEADER", file=f)
                self.connection.commit()
            except Error as e:
                print("Error: Unable to copy the data.")
                print(e)
                self.connection.rollback()

    def execute_query(self, data, name="default"):
        sql = f"SELECT * FROM {self.point_table} WHERE sfc_head IN %(data)s"
        self.cursor.execute(sql, {'data': tuple(data)})
        results = self.cursor.fetchall()

        for row in results:
            print(row)

    def create_btree_index(self, name="default"):
        sql = f"CREATE INDEX {self.btree_index} ON {self.point_table} USING btree (sfc_head)"
        try:
            self.cursor.execute(sql)
            self.connection.commit()
        except Error as e:
            print(f"Error: Unable to execute query: {sql}")
            print(e)
            self.connection.rollback()
