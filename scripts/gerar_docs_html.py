"""Gera um site estático (um único HTML) com a documentação do projeto.

Lê os Markdown do repositório (CLAUDE.md, docs/**, READMEs de src/), embute o texto no HTML e
renderiza no navegador com `marked`. Nada é escrito à mão: rodar de novo após editar os .md.

Uso (da raiz do repo):
    poetry run python scripts/gerar_docs_html.py            # → docs/_site/index.html
    poetry run python scripts/gerar_docs_html.py --artifact # sem <html>/<head> (publicação como Artifact)
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from datetime import date
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
SAIDA_PADRAO = RAIZ / "docs" / "_site" / "index.html"

# Grupos da navegação, na ordem em que aparecem. Cada .md cai no primeiro grupo cujo teste aceitar.
GRUPOS = [
    ("Projeto", lambda p: p in {"CLAUDE.md", "docs/architecture.md", "docs/glossario.md",
                                 "docs/decisions.md", "docs/backlog.md"}),
    ("Specs por camada", lambda p: p.startswith("docs/specs/") and p.count("/") == 2),
    ("Features", lambda p: p.startswith("docs/specs/features/")),
    ("Código", lambda p: p.startswith("src/")),
    ("TCC", lambda p: p.startswith("docs/")),
]
ORDEM = ["CLAUDE.md", "docs/architecture.md", "docs/glossario.md", "docs/decisions.md", "docs/backlog.md",
         "docs/specs/landing.md", "docs/specs/bronze.md", "docs/specs/silver_intermediate.md",
         "docs/specs/silver_final.md", "docs/specs/gold.md", "docs/specs/_template.md",
         "docs/tcc.md", "docs/mineracao_receita.md"]


def listar_markdown() -> list[str]:
    caminhos = ["CLAUDE.md"]
    caminhos += [p.relative_to(RAIZ).as_posix() for p in (RAIZ / "docs").rglob("*.md") if "_site" not in p.parts]
    caminhos += [p.relative_to(RAIZ).as_posix() for p in (RAIZ / "src").rglob("*.md")
                 if p.name.lower() == "readme.md"]
    return sorted(set(caminhos), key=lambda c: (ORDEM.index(c) if c in ORDEM else len(ORDEM), c))


def slug(caminho: str) -> str:
    base = caminho.removesuffix(".md").removeprefix("docs/")
    return re.sub(r"[^a-z0-9]+", "-", base.lower()).strip("-") or "doc"


TITULOS_FIXOS = {"docs/specs/_template.md": "Modelo de spec de feature",
                 "src/silver_intermediate/readme.md": "Silver intermediate — readme do código"}


def titulo(texto: str, caminho: str) -> str:
    if caminho in TITULOS_FIXOS:
        return TITULOS_FIXOS[caminho]
    m = re.search(r"^#\s+(.+)$", texto, re.M)
    return re.sub(r"[`*]", "", m.group(1)).strip() if m else caminho


def resumo(texto: str) -> str:
    """Primeiro parágrafo depois do título (sem tabelas/listas), para os cartões da visão geral."""
    for bloco in re.split(r"\n\s*\n", texto):
        b = bloco.strip()
        if not b or b.startswith(("#", "|", "```", "-", "*")):
            continue
        b = re.sub(r"^>\s?", "", b, flags=re.M)
        b = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", b)
        b = re.sub(r"[*`_]", "", " ".join(b.split()))
        return b[:220] + ("…" if len(b) > 220 else "")
    return ""


def estado_backlog(texto: str) -> dict:
    """Conta itens da tabela de bugs: abertos por severidade e resolvidos (riscados)."""
    linhas = [l for l in texto.splitlines() if re.match(r"\|\s*(~~)?B\d+", l)]
    abertos = [l for l in linhas if not l.lstrip("| ").startswith("~~")]
    return {
        "criticos": sum("🔴" in l.split("|")[1] for l in abertos),
        "altos": sum("🟠" in l.split("|")[1] for l in abertos),
        "abertos": len(abertos),
        "resolvidos": len(linhas) - len(abertos),
    }


def git(*args: str) -> str:
    try:
        return subprocess.run(["git", *args], cwd=RAIZ, capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "?"


def coletar() -> dict:
    docs = []
    for caminho in listar_markdown():
        texto = (RAIZ / caminho).read_text(encoding="utf-8")
        grupo = next(nome for nome, teste in GRUPOS if teste(caminho) or nome == "TCC")
        docs.append({"caminho": caminho, "slug": slug(caminho), "titulo": titulo(texto, caminho),
                     "grupo": grupo, "resumo": resumo(texto), "md": texto})
    backlog = next((d["md"] for d in docs if d["caminho"] == "docs/backlog.md"), "")
    return {
        "docs": docs,
        "grupos": [g for g, _ in GRUPOS],
        "backlog": estado_backlog(backlog),
        "gerado_em": date.today().isoformat(),
        "branch": git("rev-parse", "--abbrev-ref", "HEAD"),
        "commit": git("rev-parse", "--short", "HEAD"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--saida", type=Path, default=SAIDA_PADRAO)
    parser.add_argument("--artifact", action="store_true", help="omite <!doctype>/<html>/<head>/<body>")
    args = parser.parse_args()

    dados = json.dumps(coletar(), ensure_ascii=False).replace("</", "<\\/")
    corpo = (Path(__file__).with_name("docs_html_template.html").read_text(encoding="utf-8")
             .replace("/*__DADOS__*/null", dados))
    if not args.artifact:
        corpo = ("<!doctype html>\n<html lang=\"pt-BR\">\n<head><meta charset=\"utf-8\">"
                 "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1, viewport-fit=cover\">"
                 "</head>\n<body>\n" + corpo + "\n</body>\n</html>\n")
    args.saida.parent.mkdir(parents=True, exist_ok=True)
    args.saida.write_text(corpo, encoding="utf-8")
    print(f"✔ {args.saida.relative_to(RAIZ) if args.saida.is_relative_to(RAIZ) else args.saida}")


if __name__ == "__main__":
    main()
