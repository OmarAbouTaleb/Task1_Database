from psycopg2 import pool
from psycopg2.extras import RealDictCursor

# Connection string for Neon database
connection_string = postgresql:"//neondb_owner:VX8cA4OFpPZB@ep-steep-cake-a5nzzfut.us-east-2.aws.neon.tech/neondb?sslmode=require"

# Create a connection pool
connection_pool = pool.SimpleConnectionPool(
    1,  # Minimum number of connections in the pool
    10,  # Maximum number of connections in the pool
    connection_string
)

if connection_pool:
    print("Connection pool created successfully")


def get_supervisor_data():
    query = """
    SELECT 
        supervisor.name AS supervisor_name, 
        supervisor.age AS supervisor_age, 
        COUNT(employee.id) AS employee_count
    FROM Employee employee
    LEFT JOIN Employee supervisor
        ON employee.supervisor_id = supervisor.id
    WHERE supervisor.id IS NOT NULL
    GROUP BY supervisor.id
    ORDER BY supervisor.name;
    """
    try:
        # Get a connection from the pool
        conn = connection_pool.getconn()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Execute the query
        cursor.execute(query)
        result = cursor.fetchall()

        # Print the results
        print("Supervisors and their supervised employee count:")
        for row in result:
            print(f"Supervisor Name: {row['supervisor_name']}, Age: {row['supervisor_age']}, Employee Count: {row['employee_count']}")
    
    except Exception as e:
        print(f"Error: {e}")
    
    finally:
        # Release the connection back to the pool
        if cursor:
            cursor.close()
        if conn:
            connection_pool.putconn(conn)


if _name_ == "_main_":  # Correct main function declaration
    get_supervisor_data()