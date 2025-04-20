from typing import Any, Dict

import mysql.connector
from mysql.connector import Error

from app.tool.base import BaseTool


class MySQLStoredProcedureFunction(BaseTool):
    name : str = "mysql_execute_procedure"
    description : str = "Execute stored procedure in MySQL database"
    parameters : dict = {
        "type": "object",
        "properties": {
            "connection": {
                "type": "object",
                "properties": {
                    "host": {"type": "string", "default": "localhost"},
                    "port": {"type": "integer", "default": 3306},
                    "user": {"type": "string", "default": "root"},
                    "password": {"type": "string", "default": ""},
                    "database": {"type": "string", "default": "openmanus"}
                },
            },
            "procedure": {"type": "string"},
            "parameters": {
                "type": "object",
                "additionalProperties": {"type": "string"}
            },
        },
        "required": ["connection", "procedure"]
    }

    async def execute(self, connection: Dict[str, Any], procedure: str, parameters: object) -> Dict[str, Any]:
        conn = None
        cursor = None
        try:
            # 建立数据库连接
            conn = mysql.connector.connect(
                host=connection["host"],
                port=connection["port"],
                user=connection["user"],
                password=connection["password"],
                database=connection["database"]
            )

            # 创建游标
            cursor = conn.cursor(dictionary=True)
            cursor.callproc(procedure, list(parameters.values()))
            results = []
            for result in cursor.stored_results():
                results.append(result.fetchall())

            return {
                "status": "success",
                "data": results
            }

        except Error as e:
            return {
                "status": "error",
                "message": str(e)
            }
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

