from typing import Any, Dict
import mysql.connector
from mysql.connector import Error
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error

from app.tool.base import BaseTool

class DataAnalysisFunction(BaseTool):
    name: str = "data_analysis"
    description: str = "Perform data analysis on local data selected from mysql database."
    parameters: dict = {
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
                "required": ["host", "port", "user", "password", "database"]
            },
            "analysis_type": {"type": "string", "enum": ["descriptive", "correlation", "linear_regression"], "description": "Type of analysis to perform"},
            "query": {"type": "string", "description": "SQL query to execute to retrieve data from the database"}
        },
        "required": ["connection", "analysis_type", "query"]
    }

    async def execute(self, connection: Dict[str, Any], analysis_type: str, query: str) -> Dict[str, Any]:
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

            cursor = conn.cursor(dictionary=True)
            cursor.execute(query)

            data = pd.DataFrame(cursor.fetchall())
            numeric_cols = data.select_dtypes(include=["float64", "int64"]).columns
            data = data[numeric_cols]

            # 缺失值填充
            for col in numeric_cols:
                mean_value = data[col].mean()
                data[col].fillna(mean_value, inplace=True)

            if analysis_type == "descriptive":
                # 描述性统计分析
                return {
                    "status": "success",
                    "data": data.describe()
                }
            elif analysis_type == "correlation":
                # 计算数据的相关性
                return {
                    "status": "success",
                    "data": data.corr()
                }
            elif analysis_type == "linear_regression":
                # 线性回归分析，这里假设数据中有'target'作为目标变量
                target_col = 'target'
                if target_col not in data.columns:
                    target_col = data.columns[-1]
                
                X = data.drop(target_col, axis=1)
                y = data[target_col]
                X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
                
                # 创建线性回归模型
                model = LinearRegression()
                model.fit(X_train, y_train)
                
                # 预测
                y_pred = model.predict(X_test)
                
                # 评估模型
                mse = mean_squared_error(y_test, y_pred)
                return {
                    "status": "success",
                    "data": {
                        "model_coefficients": model.coef_,
                        "model_intercept": model.intercept_,
                        "mean_squared_error": mse
                    }
                }
            else:
                return {
                    "status": "error",
                    "message": f"Unsupported analysis type: {analysis_type}"
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
