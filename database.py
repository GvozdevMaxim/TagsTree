import mysql.connector


class DbConnection:

    def __init__(self, database, user, password, host):
        self.__database = database
        self.__user = user
        self.__password = password
        self.__host = host

    def db_try_to_connect(self):
        try:
            return mysql.connector.connect(database=self.__database, user=self.__user, password=self.__password,
                                           host=self.__host)

        except mysql.connector.Error as err:
            print(err)

    @staticmethod
    def get_projects(project_id):
        project_query = f'SELECT id FROM project WHERE project_id = {project_id}'
        with conn.cursor() as curs:
            try:
                curs.execute(project_query)
                return curs.fetchall()

            except mysql.connector.Error as err:
                print(err)

    @staticmethod
    def get_publications(proj_id):
        publication_query = f'''SELECT publication_id, content FROM publication P, publications_in_project Pp, project Pj
                                WHERE P.publication_id  = Pp.publications_id
                                AND Pp.project_id  = Pj.id
                                AND Pj.id = {proj_id};'''
        with conn.cursor() as curs:

            try:
                curs.execute(publication_query)
                return curs.fetchall()


            except mysql.connector.Error as err:
                print("Some other error")
                print(err)

    @staticmethod
    def insert_new_project(project_collections):
        with conn.cursor() as curs:

            project_query = f"INSERT INTO project (project_id, description, period) VALUES (%s, %s, %s) "
            try:
                if len(project_collections) == 1:
                    curs.execute(project_query, project_collections[0])
                else:
                    curs.executemany(project_query, project_collections)
                conn.commit()
                print(f"{len(project_collections)} project(s) has/have been successfully inserted")

            except mysql.connector.Error as err:
                print(err)

    @staticmethod
    def insert_new_tags(tags_collection):
        with conn.cursor() as curs:
            # Подготовка запроса для вставки/обновления
            tags_query = """INSERT INTO tags (project_id, tag, count, publication_id)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    count = VALUES(count),
                    publication_id = VALUES(publication_id)"""

            values = []
            for tag_data in tags_collection:
                project_id = tag_data[0]
                tags_data = tag_data[1:]
                for i in range(0, len(tags_data), 3):
                    tag = tags_data[i]
                    tag_count = tags_data[i + 1]
                    publication_id = tags_data[i + 2]

                    values.append((project_id, tag, tag_count, publication_id))

            try:
                curs.executemany(tags_query, values)
                conn.commit()
                print("Tags have been successfully inserted or updated.")
            except mysql.connector.Error as err:
                print(f"Error: {err}")
            finally:
                conn.close()

    @staticmethod
    def insert_into_publication_and_publications_in_project(project_id, period, collection):

        with conn.cursor() as curs:
            try:
                proj_id = curs.execute(f'SELECT id FROM project WHERE period=%s AND project_id=%s',
                                       (period, project_id,))
                proj_id = curs.fetchone()[0]

            except mysql.connector.Error as err:
                print("Some other error")
                print(err)

            try:

                curs.execute("BEGIN")
                publication_query = f"INSERT INTO publication (title, content, data, source) VALUES (%s, %s, %s, %s) "
                publication_in_project_query = f"INSERT INTO publications_in_project (project_id, publications_id) VALUES (%s, %s)"

                try:
                    curs.execute(publication_query, collection)
                    newid = curs.lastrowid


                except mysql.connector.Error as err:
                    print(err)

                try:
                    publication_in_project_collection = (proj_id, newid)
                    curs.execute(publication_in_project_query, publication_in_project_collection)

                except mysql.connector.Error as err:
                    print(err)

                conn.commit()

            except print('conn begin fail'):
                try:  # empty exception handler in case rollback fails
                    conn.rollback()
                    print('rallback')
                except:
                    pass
            else:
                print(f"{len(collection)} records inserted successfully")


dbconnection = DbConnection(database='tagclouddb', user='gastinha', password='Gastinh@', host='localhost')
conn = dbconnection.db_try_to_connect()
