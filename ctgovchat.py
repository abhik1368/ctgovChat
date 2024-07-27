from pydantic import BaseModel
from pydantic_settings import BaseSettings
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from openai import Client
import prompt_toolkit
from prompt_toolkit.completion import WordCompleter
import time
from tabulate import tabulate

class PostgresConnection(BaseModel):
    host: str
    port: int
    database: str
    user: str
    password: str

class Config(BaseSettings):
    openAIAPIKey: str
    openAIModel: str
    dbTimeoutMs: int
    apiTimeoutMs: int
    postgresConnection: PostgresConnection

    @classmethod
    def from_json(cls, json_file: str):
        with open(json_file, 'r') as f:
            data = json.load(f)
        return cls(**data)

def connect_db(config: Config):
    conn = psycopg2.connect(
        host=config.postgresConnection.host,
        port=config.postgresConnection.port,
        database=config.postgresConnection.database,
        user=config.postgresConnection.user,
        password=config.postgresConnection.password
    )
    return conn

def get_schema(conn):
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("""
             SELECT
                table_name,
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM
                information_schema.columns
            WHERE
                table_schema = 'ctgov'
            ORDER BY
                ordinal_position;
        """)
        schema = cursor.fetchall()
        return schema

def get_example_rows(conn, tables):
    example_rows = {}
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        for table_name in tables:
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 1;")
            example_row = cursor.fetchone()
            example_rows[table_name] = example_row
    return example_rows

def format_schema(schema, example_rows):
    tables = {}
    for row in schema:
        table_name = row['table_name']
        if table_name not in tables:
            tables[table_name] = []
        tables[table_name].append(row)

    table_strings = []
    for table_name, columns in tables.items():
        column_strings = []
        for column in columns:
            print(column)
            example_value = 'N/A' if example_rows[table_name] is None else example_rows[table_name].get(column['column_name'], 'N/A')
            column_string = f"  {column['column_name']} {column['data_type']} {'NULL' if column['is_nullable'] == 'YES' else 'NOT NULL'}; Example: {example_value}"
            column_strings.append(column_string)
        table_string = f"Table {table_name}:\n" + "\n".join(column_strings)
        table_strings.append(table_string)

    return '\n\n'.join(table_strings)    

def ask_openai(prompt: str, config: Config):
    client = Client(api_key=config.openAIAPIKey)
    response = client.chat.completions.create(
        model=config.openAIModel,
        messages=[
            {"role": "system", "content": "You are a database analyst and data scientist."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.1,
        max_tokens=2500
    )
    return response['choices'][0]['message']['content'].strip()

def extract_queries_from_response(response_content: str):
    chunks = response_content.split("```")
    queries = [chunk.replace("sql", "").strip() for i, chunk in enumerate(chunks) if i % 2 != 0]
    return queries

def connect_db(config: Config):
    conn = psycopg2.connect(
        host=config.postgresConnection.host,
        port=config.postgresConnection.port,
        database=config.postgresConnection.database,
        user=config.postgresConnection.user,
        password=config.postgresConnection.password
    )
    return conn

def main():
    # Provide the path to the config file directly
    config_file = 'config.json'
    config = Config.from_json(config_file)

    # Connect to the database
    conn = connect_db(config)

    # Extract and print schema
    schema = get_schema(conn)
    tables = {row['table_name'] for row in schema}
    example_rows = get_example_rows(conn, tables)
    schema_string = format_schema(schema, example_rows)
    
    print(f"\nYou are connected to the database {config.postgresConnection.database}. It has the following tables:\n\n{', '.join(tables)}\n")
    print(f"Schema and example rows:\n\n{schema_string}")

    # Create a completer for table names
    table_completer = WordCompleter(tables, ignore_case=True)

    # Prompt user to select a table
    selected_table = prompt_toolkit.prompt("Select a table: ", completer=table_completer)
    if selected_table not in tables:
        print("Invalid table name. Exiting.")
        return

    # Create a completer for column names of the selected table
    columns = [row['column_name'] for row in schema if row['table_name'] == selected_table]
    column_completer = WordCompleter(columns, ignore_case=True)

    initial_question = prompt_toolkit.prompt("Ask me a question about this database, and I'll try to answer! (q to quit): ", completer=column_completer)
    if initial_question.lower() == 'q':
        return

    messages = [
        {"role": "system", "content": "You are a helpful assistant that writes SQL queries in order to answer questions about a database."},
        {"role": "user", "content": f"Hello, I have a database with the following schema:\n\n{schema_string}\n\nI'd like to work with you to answer a question I have. I can run several queries to get the answer, and tell you the results along the way. I'd like to use the fewest queries possible, so use joins where you can. If you're not sure what to do, you can ask me questions about the database or run intermediate queries to learn more about the data, but I can only run one query at a time.\n\nThe question I have is:\n\n\"{initial_question}\""}
    ]

    while True:
        print("Calling GPT...")
        start_time = time.time()

        num_tries = 0
        response = None
        while num_tries < 4:
            try:
                response = Client(api_key=config.openAIAPIKey).chat.completions.create(
                    model=config.openAIModel,
                    messages=messages,
                    timeout=config.apiTimeoutMs
                )
                break
            except Exception as e:
                print(f"ERROR: {e}")
                num_tries += 1
                if num_tries >= 3:
                    print("Giving up.")
                    return

        end_time = time.time()
        #print(f"Took {end_time - start_time:.2f} seconds. Used {response['usage']['total_tokens']} tokens so far.")

        response_content = response.choices[0].message.content
        print(f"\nASSISTANT:\n\n{response_content}\n")

        messages.append({"role": "assistant", "content": response_content})

        queries = extract_queries_from_response(response_content)
        result_string = ""

        for query in queries:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(query)
                    result = cursor.fetchall()
                    print(f"Returned {len(result)} rows. Here are the first few rows:")
                    print(tabulate(result, headers="keys"))
                    result_string += f"\n\nI ran `{query}` and it returned {len(result)} rows. Here are the first few rows:\n{tabulate(result, headers='keys', tablefmt='plain')}\n"
            except Exception as e:
                print(f"Error: {e}")
                result_string += f"\nResult for `{query}` was an error: {e}\n"

        next_message = prompt_toolkit.prompt("How would you like to respond? Any query results will be automatically sent with your response. (q to quit): ", completer=column_completer)
        if next_message.lower() == 'q':
            break

        messages.append({"role": "user", "content": f"\n{next_message}\n{result_string}"})

    conn.close()

if __name__ == "__main__":
    main()
