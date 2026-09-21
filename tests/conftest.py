# tests/conftest.py
import os

# Injeta variáveis de ambiente "dummy" ANTES do Pytest importar os seus módulos
os.environ["TOKEN_SHARE"] = "test_token_dummy"
os.environ["AWS_BUCKET_NAME"] = "test_bucket_dummy"