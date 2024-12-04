import psycopg2

def connect_to_database():
    """Create a connection to the PostgreSQL database."""
    try:
        connection = psycopg2.connect(
            dbname='postgres',
            user='postgres',
            password='YourSecurePassword',
            host='localhost',
            port=5432
        )
        connection.autocommit = False  # Disable autocommit for transactional integrity
        print("Successfully connected to the database.")
        return connection
    except psycopg2.Error as error:
        print(f"Connection error: {error}")
        return None

def execute_transaction(connection, sql_queries):
    """
    Execute a sequence of SQL commands within a single transaction.
    :param connection: The database connection object.
    :param sql_queries: A list of SQL statements to be executed.
    """
    try:
        with connection.cursor() as cursor:
            for query in sql_queries:
                print(f"Executing: {query.strip()}")
                cursor.execute(query)
            connection.commit()  # Commit changes if all queries execute successfully
            print("Transaction completed successfully.")
    except Exception as error:
        connection.rollback()  # Revert changes on error
        print(f"Transaction failed. Rolled back changes. Error: {error}")

def manage_transactions():
    # Connect to the PostgreSQL database
    db_connection = connect_to_database()

    if db_connection:
        # Define SQL commands for each transaction
        transaction_list = [
            # Insert product and stock
            [
                """
                INSERT INTO Product (prod_id, pname, price)
                SELECT 'p100', 'cd', 5
                WHERE NOT EXISTS (SELECT 1 FROM Product WHERE prod_id = 'p100');
                """,
                """
                INSERT INTO Stock (prod_id, dep_id, quantity)
                SELECT 'p100', 'd2', 50
                WHERE NOT EXISTS (SELECT 1 FROM Stock WHERE prod_id = 'p100' AND dep_id = 'd2');
                """
            ],
            # Insert depot and update stock
            [
                """
                INSERT INTO Depot (dep_id, addr, volume)
                SELECT 'd100', 'Chicago', 100
                WHERE NOT EXISTS (SELECT 1 FROM Depot WHERE dep_id = 'd100');
                """,
                """
                INSERT INTO Stock (prod_id, dep_id, quantity)
                SELECT 'p1', 'd100', 100
                WHERE EXISTS (SELECT 1 FROM Product WHERE prod_id = 'p1')
                AND NOT EXISTS (SELECT 1 FROM Stock WHERE prod_id = 'p1' AND dep_id = 'd100');
                """
            ],
            # Rename product ID
            [
                "UPDATE Stock SET prod_id = 'pp1' WHERE prod_id = 'p1';",
                "UPDATE Product SET prod_id = 'pp1' WHERE prod_id = 'p1';"
            ],
            # Rename depot ID
            [
                "UPDATE Stock SET dep_id = 'dd1' WHERE dep_id = 'd1';",
                "UPDATE Depot SET dep_id = 'dd1' WHERE dep_id = 'd1';"
            ],
            # Delete product
            [
                "DELETE FROM Stock WHERE prod_id = 'p1';",
                "DELETE FROM Product WHERE prod_id = 'p1';"
            ],
            # Delete depot
            [
                "DELETE FROM Stock WHERE dep_id = 'd1';",
                "DELETE FROM Depot WHERE dep_id = 'd1';"
            ]
        ]

        # Process each transaction
        for index, queries in enumerate(transaction_list, start=1):
            print(f"\nProcessing Transaction {index}:")
            execute_transaction(db_connection, queries)

        # Close the database connection
        db_connection.close()
        print("Database connection closed.")

if __name__ == "__main__":
    manage_transactions()