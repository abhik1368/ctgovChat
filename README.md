# ctgovchat.py

This script is designed to connect to a PostgreSQL database, retrieve schema information, and interact with an OpenAI model to generate SQL queries based on user questions about the database.

## Features

- **Database Connection**: Connects to a PostgreSQL database using configuration details provided in a JSON file.
- **Schema Extraction**: Retrieves and formats the schema of the database, including example rows from each table.
- **Interactive Querying**: Uses OpenAI's API to generate SQL queries based on user input and provides results back to the user.
- **Prompt Completion**: Provides autocompletion for table and column names to facilitate user interaction.

## Requirements

- Python 3.7+
- Required Python packages:
  - `pydantic`
  - `psycopg2`
  - `openai`
  - `prompt_toolkit`
  - `tabulate`

## Configuration

Create a JSON configuration file (e.g., `config2.json`) with the following structure:

\`\`\`json
{
  "openAIAPIKey": "your-openai-api-key",
  "openAIModel": "model-name",
  "dbTimeoutMs": 5000,
  "apiTimeoutMs": 10000,
  "postgresConnection": {
    "host": "your-db-host",
    "port": 5432,
    "database": "your-db-name",
    "user": "your-db-user",
    "password": "your-db-password"
  }
}
\`\`\`

## Usage

1. **Prepare Configuration File**: Ensure you have your configuration file (`config2.json`) ready with appropriate details.
2. **Run the Script**: Execute the script using Python:
   \`\`\`bash
   python ctgovchat.py
   \`\`\`
3. **Interact with the Script**: Follow the prompts to select a table and ask questions. The script will use OpenAI to generate SQL queries and provide results.
