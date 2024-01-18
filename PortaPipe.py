import json
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, JSON, BigInteger, SmallInteger, Table, MetaData, ForeignKey, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()
#super sexy type selector
def get_sqlalchemy_type(value):
    if isinstance(value, int):
        if value > 2147483647 or value < -2147483648:
            return BigInteger
        elif value > 32767 or value < -32768:
            return Integer
        elif value > 127 or value < -128:
            return SmallInteger
        else:
            return SmallInteger
    elif isinstance(value, float):
        return Float
    elif isinstance(value, bool):
        return Boolean
    elif isinstance(value, str):
        return String(255)
    elif isinstance(value, list) or isinstance(value, dict):
        return JSON
    else:
        return String(255)

def create_table(table_name, columns_types, primary_key_columns=None, unique_columns=None):
    class DynamicTable(Base):
        __tablename__ = table_name
        __table_args__ = {'extend_existing': True}

    for column, data_type in columns_types.items():
        setattr(DynamicTable, column, Column(column, data_type))
    if primary_key_columns:
        primary_key_constraint = PrimaryKeyConstraint(*primary_key_columns, name=f'pk_{table_name}')
        setattr(DynamicTable, '__table_args__', (*getattr(DynamicTable, '__table_args__', ()), primary_key_constraint))
    if unique_columns:
        unique_constraint = UniqueConstraint(*unique_columns, name=f'uc_{table_name}')
        setattr(DynamicTable, '__table_args__', (*getattr(DynamicTable, '__table_args__', ()), unique_constraint))

    return DynamicTable

def generate_advanced_sql_statements(json_data):
    sql_statements = []
    metadata = MetaData()
    for table_name, rows in json_data.items():
        columns_types = {column: get_sqlalchemy_type(rows[0][column]) for column in rows[0].keys()}
        primary_key_columns = [column for column, data_type in columns_types.items() if len(set(row[column] for row in rows)) == len(rows)]
        unique_columns = [column for column, data_type in columns_types.items() if len(set(row[column] for row in rows)) == len(rows) and column not in primary_key_columns]
        DynamicTable = create_table(table_name, columns_types, primary_key_columns, unique_columns)
        create_table_statement = str(DynamicTable.__table__.create(bind=None, checkfirst=True)).strip() + ';'
        sql_statements.append(create_table_statement)

        # Generate INSERT INTO statements
        for row in rows:
            columns = ', '.join(columns_types.keys())
            values = ', '.join([f"'{row[column]}'" if isinstance(row[column], str) else str(row[column]) for column in columns_types.keys()])
            insert_statement = f"INSERT INTO {table_name} ({columns}) VALUES ({values});"
            sql_statements.append(insert_statement)

    return sql_statements

with open('input.json') as json_file:
    json_data = json.load(json_file)

sql_statements = generate_advanced_sql_statements(json_data)

with open('output.sql', 'w') as sql_file:
    sql_file.write('\n'.join(sql_statements))
