#!/usr/bin/env python3
"""
Simple script to run the FastAPI server
"""
import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=9001,
        reload=True,  # Set to False for production
        log_level="info",
    )

