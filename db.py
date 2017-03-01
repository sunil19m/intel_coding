from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine('sqlite:///commands.db')
session = sessionmaker(bind=engine)()
