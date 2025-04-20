from typing import Dict

import mysql.connector
from mysql.connector import Error

from app.tool.base import BaseTool


class MySQLQueryFunction(BaseTool):
    name : str = "mysql_query"
    description : str = "Execute SQL query on MySQL database"
    parameters : dict = {
        "type": "object",
        "properties": {
            "host": {"type": "string", "description": "MySQL server host"},
            "port": {"type": "integer", "description": "MySQL server port"},
            "user": {"type": "string", "description": "MySQL server username"},
            "password": {"type": "string", "description": "MySQL server password"},
            "database": {"type": "string", "description": "MySQL database name"},
            "query": {"type": "string", "description": "SQL query to execute"}
        },
        "required": ["query"]
    }

    async def execute(self, query: str, host: str = "localhost", port: int = 3306, user: str = "root", password: str = "", database: str = "openmanus") -> Dict:
        try:
            connection = mysql.connector.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database
            )

            cursor = connection.cursor(dictionary=True)
            cursor.execute(query)

            result = cursor.fetchall()
            cursor.close()
            connection.close()

            return {
                "status": "success",
                "data": result
            }
        except Error as e:
            return {"status": "error", "message": str(e)}
