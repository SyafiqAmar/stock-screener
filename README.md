# IDX Stock Screener System

Modern, asynchronous stock screening system for the Indonesia Stock Exchange (IDX).

## Core Features
- **Market-Wide Scanning**: Analyzes ~800+ tickers across multiple timeframes (1h, 4h, 1d).
- **Advanced Technical Analysis**:
    - Bullish & Hidden Bullish Divergence.
    - Elliott Wave ABC Correction patterns.
    - Accumulation/Distribution (ADL/OBV/MFI) analysis.
- **Top 5 Ranking**: Automatically identifies and ranks the best trading setups every hour.
- **Telegram Notifications**: Consolidated hourly summaries with Buy Area, Stop Loss, and Take Profit recommendations.
- **Modern Architecture**:
    - **FastAPI** backend for high-performance API access.
    - **PostgreSQL (Async)** for robust data storage.
    - **Dockerized** environments for seamless deployment.

## Getting Started

### 1. Prerequisites
- Docker & Docker Compose installed.

### 2. Configuration
Create a `.env` file in the root directory:
```env
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
DATABASE_URL=postgresql+asyncpg://postgres:postgres@db:5432/stock_screener
REDIS_URL=redis://redis:6379/0
```

### 3. Deployment
```bash
docker-compose up -d
```

### 4. Data Import
To populate the database with all IDX tickers, place `list-company.xlsx` in the `data/` folder and run:
```bash
docker exec stock_screener_app python backend/scripts/import_idx_tickers.py
```

### 5. Access
- **Dashboard**: http://localhost:8000
- **API Docs**: http://localhost:8000/docs
