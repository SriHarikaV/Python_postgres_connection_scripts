import psycopg2
from psycopg2.extras import DictCursor

hostname = 'localhost'
database = 'employers'
username = 'postgres'
pwd = '' 
port_id = 5432
conn = None
#cur = None

try:
    with psycopg2.connect(
            host=hostname,
            dbname=database,
            user=username,
            password=pwd,
            port=port_id) as conn:
    
        with conn.cursor(cursor_factory=DictCursor) as cur:
          

            cur.execute("DROP TABLE IF EXISTS employee")

            create_script = '''CREATE TABLE IF NOT EXISTS employee (
                                id      int PRIMARY KEY,
                                name    varchar(40) NOT NULL,
                                salary  int,
                                dept_id varchar(30))'''
            cur.execute(create_script)

            insert_script = "INSERT INTO employee (id, name, salary, dept_id) VALUES (%s, %s, %s, %s)"  # ✅ Corrected placeholder
            insert_value = [(1, "harika", 12000, "D1"), (2, "bharath", 15000, "D1"), (3, "priya", 11000, "D2")]
            
            for record in insert_value:
                cur.execute(insert_script, record)  # ✅ Corrected execution

            update_script = "UPDATE employee SET salary = salary + (salary * 0.5)"
            cur.execute(update_script)

            delete_script = "DELETE FROM employee WHERE name = %s"
            delete_record = ("harika",)
            cur.execute(delete_script, delete_record )

            cur.execute("SELECT * FROM employee")
            for record in cur.fetchall():
                print(record["name"], record["salary"])

            #conn.commit()
except Exception as error:
    print("Error:", error)

finally:
    #if cur is not None:
        #cur.close()
    if conn is not None:
        conn.close()
