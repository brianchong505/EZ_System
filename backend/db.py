from sqlalchemy import create_engine

def get_engine():
    return create_engine(
        "mysql+pymysql://root:1234@localhost/ez_system",
        pool_recycle=3600,
        pool_pre_ping=True
    )