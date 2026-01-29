def format_sql_with_values(sql: str, params: list) -> str:
    if not params:
        return sql
    
    formatted_sql = sql
    
    for i, value in enumerate(params, start=1):
        placeholder = f':{i}'
        
        if value is None:
            formatted_value = 'NULL'
        elif isinstance(value, str):
            escaped_value = value.replace("'", "''")
            formatted_value = f"'{escaped_value}'"
        elif isinstance(value, (int, float)):
            formatted_value = str(value)
        else:
            formatted_value = f"'{str(value)}'"
        
        formatted_sql = formatted_sql.replace(placeholder, formatted_value)
    
    return formatted_sql
