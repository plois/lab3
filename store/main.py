from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import sqlalchemy
from sqlalchemy.orm import sessionmaker, declarative_base

DATABASE_URL = "postgresql+psycopg2://user:pass@postgres_db:5432/test_db"

Base = declarative_base()

class ProcessedAgentData(Base):
    __tablename__ = "processed_agent_data"

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, index=True)
    road_state = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    x = sqlalchemy.Column(sqlalchemy.Float)
    y = sqlalchemy.Column(sqlalchemy.Float)
    z = sqlalchemy.Column(sqlalchemy.Float)
    latitude = sqlalchemy.Column(sqlalchemy.Float)
    longitude = sqlalchemy.Column(sqlalchemy.Float)
    timestamp = sqlalchemy.Column(sqlalchemy.DateTime)

engine = sqlalchemy.create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class ProcessedAgentDataModel(BaseModel):
    road_state: str
    x: float
    y: float
    z: float
    latitude: float
    longitude: float
    timestamp: str

app = FastAPI()

@app.post("/processed_agent_data/")
def save_processed_agent_data(data: List[ProcessedAgentDataModel]):
    db = SessionLocal()
    try:
        for item in data:
            db_data = ProcessedAgentData(
                road_state=item.road_state,
                x=item.x,
                y=item.y,
                z=item.z,
                latitude=item.latitude,
                longitude=item.longitude,
                timestamp=item.timestamp
            )
            db.add(db_data)
        db.commit()
        return {"status": "success"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()
