import os
from os.path import join, dirname
from dotenv import load_dotenv
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from models import create_tables, Publisher, Shop, Book, Stock, Sale
import json


def data_load():
    with open('tests_data1.json', 'r') as f:
        data = json.load(f)
    for record in data:
        model = {
            'publisher': Publisher,
            'shop': Shop,
            'book': Book,
            'stock': Stock,
            'sale': Sale
        }[record.get('model')]
        session.add(model(id=record.get('pk'), **record.get('fields')))
    session.commit()


def get_sales():
    publisher = input('Введите имя автора или его id')
    try:
        int(publisher)
        q = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale). \
            join(Publisher, Publisher.id == Book.id_publisher). \
            join(Stock, Stock.id_book == Book.id). \
            join(Shop, Shop.id == Stock.id_shop). \
            join(Sale, Sale.id_stock == Stock.id). \
            filter(Publisher.id == publisher)

    except ValueError:
        q = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale). \
            join(Publisher, Publisher.id == Book.id_publisher). \
            join(Stock, Stock.id_book == Book.id). \
            join(Shop, Shop.id == Stock.id_shop). \
            join(Sale, Sale.id_stock == Stock.id).\
            filter(Publisher.name == publisher)
    for s in q:
        print(*s, sep=' | ')


if __name__ == '__main__':
    dotenv_path = join(dirname(__file__), 'loginpass.env')
    load_dotenv(dotenv_path)
    login = os.environ.get("LOGIN")
    password = os.environ.get("PASS")
    DSN = f"postgresql://{login}:{password}@localhost:5432/ORMhw"
    engine = sqlalchemy.create_engine(DSN)
    create_tables(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    data_load()
    get_sales()
    session.close()
