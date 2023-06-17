from packer     import Packer
from db_manager import DBManager

def update_all():
    Packer().pack()
    DBManager('./db/meteo.db', './parquet/', './csv/').commit_latest()
