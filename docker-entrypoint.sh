#!/bin/sh
set -e

echo "📄 CitizenLink Document AI service starting..."

if [ -z "$DATABASE_URL" ]; then
  echo "❌ DATABASE_URL is not set. Exiting."
  exit 1
fi

if [ -z "$INTERNAL_SECRET" ]; then
  echo "❌ INTERNAL_SECRET is not set. Exiting."
  exit 1
fi


echo "🗄️  Running Alembic migrations..."
alembic upgrade head && echo "✅ Migrations applied." || { echo "❌ Migrations failed. Exiting."; exit 1; }

echo "🚀 Starting API server on port 8002..."
exec uvicorn main:app --host 0.0.0.0 --port 8002
