.PHONY: docs-html

# Gera docs/_site/index.html (navegador da documentação) a partir dos .md do repo.
docs-html:
	poetry run python scripts/gerar_docs_html.py
