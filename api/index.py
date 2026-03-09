"""
Vercel serverless entry: expõe o app FastAPI para o runtime Python da Vercel.
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from main import app
