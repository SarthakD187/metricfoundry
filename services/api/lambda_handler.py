# services/api/lambda_handler.py
from mangum import Mangum
from app import app
handler = Mangum(app)
