# Database Query Assistant: AI-Powered Natural Language Database Access

## Overview
Database Query Assistant is an AI-powered tool that allows users to interact with databases using natural language, eliminating the need for SQL knowledge. Simply type queries in plain English, and the AI translates them into optimized SQL statements to fetch results instantly.

## Features
- **Natural Language Interface:** Query databases using plain English.
- **AI-Powered Query Translation:** Converts user input into SQL.
- **Instant Data Retrieval:** Efficiently fetches data from PostgreSQL.
- **User-Friendly Interface:** Built using Streamlit for seamless interaction.
- **Secure Database Connectivity:** Ensures safe and reliable connections to databases.

## How It Works
1. Connect to a PostgreSQL database.
2. Input a query in natural language, such as _"Show me all orders from last month."_
3. AI (powered by OpenAI and LangChain) translates it into SQL.
4. The system runs the SQL query and fetches the data.
5. Results are displayed instantly in the Streamlit interface.

## Tech Stack
- **Frontend:** Streamlit
- **Framework:** LangChain (Python-based)
- **LLM:** OpenAI for query translation
- **Database:** PostgreSQL
- **Deployment:** Render.com

## Installation

### Prerequisites:
- Python 3.8+
- PostgreSQL database
- API key for OpenAI (required for AI processing)

### Steps:
1. Clone the repository:
   ```sh
   
   git clone https://github.com/rajeevrj256/AI_Powered_Database_Manager_SQL
   cd AI_Powered_Database_Manager_SQL
   
   ```
2. Install dependencies:
```sh

pip install -r requirements.txt

```
3. Set up environment variables:
   Create a .env file and add:
   ```sh
   
   OPENAI_API_KEY=your_openai_api_key
   
   ```
4.Run the application:
  ```sh
   Streamlit run UI.py

```

## Future Enhancements
- Support for MySQL, Microsoft SQL Server, and Oracle.

- Voice-based query processing.

- AI-powered query optimization.

- Role-based access control for enhanced security.


## Demo

  https://ai-powered-database-manager-sql.onrender.com/

## Contributing
  Contributions are welcome! Feel free to open an issue or submit a pull request.
  
