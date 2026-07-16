#!/usr/bin/env python3
"""
User-management HTTP API backed by ZydecoDB.

Your app owns users, passwords, and sessions. ZydecoDB only stores byte blobs.
Set ZYDECODB_API_KEY when the database requires auth.

Start ZydecoDB:
    cp config/zydecodb.dev.toml /tmp/zydecodb.toml
    ./target/release/zydecodb serve --config /tmp/zydecodb.toml

Run this API:
    pip install -r examples/user_backend/requirements.txt
    export ZYDECODB_API_KEY=...   # if require_auth is enabled
    python3 examples/user_backend/app.py --seed

Try it:
    # Sign up Margaret
    curl -s -X POST http://127.0.0.1:8080/api/users \\
      -H 'Content-Type: application/json' \\
      -d '{"email":"margaret.chen@example.com","name":"Margaret Chen","password":"jazzbrunch"}'

    # Log in
    curl -s -X POST http://127.0.0.1:8080/api/login \\
      -H 'Content-Type: application/json' \\
      -d '{"email":"margaret.chen@example.com","password":"jazzbrunch"}'

    # Use the token from login (replace TOKEN)
    curl -s http://127.0.0.1:8080/api/me -H 'Authorization: Bearer TOKEN'

    # List everyone
    curl -s http://127.0.0.1:8080/api/users
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from flask import Flask, g, jsonify, request

from store import AuthError, Conflict, NotFound, StoreError, UserStore, open_store
from zydecodb import Client, ZydecoError

DEFAULT_ZYDECO_HOST = "127.0.0.1"
DEFAULT_ZYDECO_PORT = 9470
DEFAULT_HTTP_PORT = 8080


def create_app(zydeco_host: str, zydeco_port: int, api_key: str | None = None) -> Flask:
    app = Flask(__name__)
    app.config["ZYDECO_HOST"] = zydeco_host
    app.config["ZYDECO_PORT"] = zydeco_port
    app.config["ZYDECO_API_KEY"] = api_key

    @app.before_request
    def connect_db() -> None:
        db = Client(
            f"{app.config['ZYDECO_HOST']}:{app.config['ZYDECO_PORT']}",
            api_key=app.config["ZYDECO_API_KEY"],
        )
        g.db = db
        g.store = UserStore(db)
        # Define the collection + indexes once per process, on first request.
        if not app.config.get("SCHEMA_READY"):
            g.store.ensure_schema()
            app.config["SCHEMA_READY"] = True

    @app.teardown_request
    def close_db(_exc=None) -> None:
        db = g.pop("db", None)
        if db is not None:
            db.close()

    def bearer_token() -> str | None:
        header = request.headers.get("Authorization", "")
        if header.startswith("Bearer "):
            return header[7:].strip() or None
        return None

    @app.get("/health")
    def health():
        return jsonify({"status": "ok", "database": "zydecodb"})

    @app.post("/api/users")
    def create_user():
        body = request.get_json(silent=True) or {}
        try:
            user = g.store.create_user(
                email=body.get("email", ""),
                name=body.get("name", ""),
                password=body.get("password", ""),
            )
            return jsonify(user.public_view()), 201
        except Conflict as exc:
            return jsonify({"error": str(exc)}), 409
        except StoreError as exc:
            return jsonify({"error": str(exc)}), 400

    @app.get("/api/users")
    def list_users():
        return jsonify({"users": g.store.list_users()})

    @app.get("/api/users/<user_id>")
    def get_user(user_id: str):
        try:
            return jsonify(g.store.get_user(user_id).public_view())
        except NotFound as exc:
            return jsonify({"error": str(exc)}), 404

    @app.patch("/api/users/<user_id>")
    def update_user(user_id: str):
        body = request.get_json(silent=True) or {}
        try:
            user = g.store.update_user_name(user_id, body.get("name", ""))
            return jsonify(user.public_view())
        except NotFound as exc:
            return jsonify({"error": str(exc)}), 404
        except StoreError as exc:
            return jsonify({"error": str(exc)}), 400

    @app.delete("/api/users/<user_id>")
    def delete_user(user_id: str):
        try:
            g.store.delete_user(user_id)
            return "", 204
        except NotFound as exc:
            return jsonify({"error": str(exc)}), 404

    @app.post("/api/login")
    def login():
        body = request.get_json(silent=True) or {}
        try:
            token, user = g.store.login(
                email=body.get("email", ""),
                password=body.get("password", ""),
            )
            return jsonify({"token": token, "user": user.public_view()})
        except AuthError as exc:
            return jsonify({"error": str(exc)}), 401
        except StoreError as exc:
            return jsonify({"error": str(exc)}), 400

    @app.get("/api/me")
    def me():
        token = bearer_token()
        if not token:
            return jsonify({"error": "missing Authorization: Bearer <token>"}), 401
        try:
            return jsonify(g.store.user_for_token(token).public_view())
        except AuthError as exc:
            return jsonify({"error": str(exc)}), 401
        except NotFound:
            return jsonify({"error": "session user no longer exists"}), 401

    @app.post("/api/logout")
    def logout():
        token = bearer_token()
        if token:
            g.store.logout(token)
        return "", 204

    @app.errorhandler(ZydecoError)
    def zydeco_error(exc: ZydecoError):
        return jsonify({"error": f"database error: {exc}"}), 503

    return app


def seed_demo_users(store: UserStore) -> None:
    """Create a couple of accounts if the database is empty."""
    if store.list_users():
        return

    demos = [
        ("margaret.chen@example.com", "Margaret Chen", "jazzbrunch"),
        ("james.roux@example.com", "James Roux", "frenchquarter"),
    ]
    for email, name, password in demos:
        try:
            store.create_user(email, name, password)
            print(f"  created demo user: {name} <{email}>")
        except Conflict:
            pass


def main() -> int:
    parser = argparse.ArgumentParser(description="User API backed by ZydecoDB")
    parser.add_argument("--zydeco-host", default=DEFAULT_ZYDECO_HOST)
    parser.add_argument("--zydeco-port", type=int, default=DEFAULT_ZYDECO_PORT)
    parser.add_argument("--http-port", type=int, default=DEFAULT_HTTP_PORT)
    parser.add_argument("--seed", action="store_true", help="insert demo users on startup")
    args = parser.parse_args()

    print(f"ZydecoDB: {args.zydeco_host}:{args.zydeco_port}")
    print(f"HTTP API: http://127.0.0.1:{args.http_port}\n")

    api_key = os.environ.get("ZYDECODB_API_KEY")

    if args.seed:
        db, store = open_store(args.zydeco_host, args.zydeco_port, api_key)
        try:
            print("Seeding demo users ...")
            seed_demo_users(store)
        finally:
            db.close()
        print()

    app = create_app(args.zydeco_host, args.zydeco_port, api_key)
    app.run(host="127.0.0.1", port=args.http_port, debug=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
