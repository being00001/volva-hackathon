#!/bin/bash
# Install SurrealDB to local bin
mkdir -p $HOME/.local/bin
export PATH=$HOME/.local/bin:$PATH
curl -sSf https://install.surrealdb.com | sh
