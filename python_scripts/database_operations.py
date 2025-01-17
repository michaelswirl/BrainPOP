from sqlalchemy import create_engine, text
import pandas as pd


class Database:
    def __init__(self, db_user, db_pass, db_host, db_port, db_name):
        """
        Set up the database connection.
        """
        self.connection_string = (
            f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
        )
        self.engine = create_engine(self.connection_string, pool_pre_ping=True)

    def execute_query(self, query, params=None):
        """
        Execute a query (e.g., INSERT, UPDATE, DELETE).
        """
        try:
            with self.engine.connect() as connection:
                connection.execute(text(query), params or {})
        except Exception as e:
            print(f"Failed to execute query: {e}")
            raise

    def fetch_query(self, query, params=None):
        """
        Run a SELECT query and return the results as a DataFrame.
        """
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(query), params or {})
                return pd.DataFrame(result.fetchall(), columns=result.keys())
        except Exception as e:
            print(f"Failed to fetch data: {e}")
            raise

    def execute_sql_file(self, file_path):
        """
        Execute all SQL statements from a file.
        """
        try:
            with self.engine.connect() as connection:
                transaction = connection.begin()
                with open(file_path, 'r') as file:
                    sql_script = file.read()
                for statement in sql_script.split(';'):
                    if statement.strip():
                        connection.execute(text(statement))
                transaction.commit()
            print(f"Executed SQL file: {file_path}")
        except Exception as e:
            print(f"Error executing SQL file '{file_path}': {e}")
            raise


    def fetch_query_from_sql_file(self, file_path, params=None):
        """
        Execute a SELECT query from a .sql file and return the results as a DataFrame.
        """
        try:
            with open(file_path, 'r') as file:
                sql_query = file.read()
            with self.engine.connect() as connection:
                result = connection.execute(text(sql_query), params or {})
                return pd.DataFrame(result.fetchall(), columns=result.keys())
        except Exception as e:
            print(f"Failed to fetch data from SQL file '{file_path}': {e}")
            raise

    def load_dataframe(self, df, table_name, if_exists="append"):
        """
        Load a DataFrame into a database table.
        """
        try:
            df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
        except Exception as e:
            print(f"Failed to load DataFrame to '{table_name}': {e}")
            raise

    def list_tables(self):
        with self.engine.connect() as connection:
            result = connection.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
            tables = result.fetchall()
            print("Tables in the database:", [table[0] for table in tables])

    def close(self):
        """
        Close the database connection.
        """
        try:
            self.engine.dispose()
            print("Database connection closed.")
        except Exception as e:
            print(f"Failed to close the connection: {e}")
            raise
