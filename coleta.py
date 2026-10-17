#!/usr/bin/env python3
"""
Coletor de apreensões de drogas sintéticas e precursores químicos (PF, PRF, Receita Federal).

Ciclo: descobrir notícias -> filtro barato por palavras-chave -> ler só as candidatas
-> extrair campos (Claude API; heurística se não houver chave) -> normalizar
-> deduplicar contra a base -> gravar JSON/CSV no formato do painel + relatório.

Uso:
  python coleta.py                      # incremental (desde a última execução, com 3 dias de folga)
  python coleta.py --desde 2026-01-01   # carga retrospectiva
  python coleta.py --fontes pf,prf      # só algumas fontes
  python coleta.py --sem-llm            # sem API (extração heurística, tudo marcado para revisão)
  python coleta.py --seco               # não grava nada, só relata
  python coleta.py --so-links           # sem custo: só descobre e filtra; acumula em links/pendentes.md
  python coleta.py --limpar-pendentes   # arquiva a lista de pendentes já processada
"""
from __future__ import annotations

import argparse
import csv
import difflib
import json
import logging
import os
import re
import time
import unicodedata
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import quote_plus, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import trafilatura  # extração de texto mais limpa (opcional)
except ImportError:
    trafilatura = None
try:
    import anthropic  # extração estruturada (recomendado)
except ImportError:
    anthropic = None

# ----------------------------------------------------------------------------- configuração
BASE = Path(__file__).resolve().parent
ARQ_BASE = BASE / "dados" / "apreensoes.json"
ARQ_CSV = BASE / "dados" / "apreensoes.csv"
ARQ_ESTADO = BASE / "dados" / "estado.json"
DIR_REL = BASE / "relatorios"
DIR_LINKS = BASE / "links"
PEND_JSON = DIR_LINKS / "pendentes.json"
PEND_MD = DIR_LINKS / "pendentes.md"

MODELO = os.getenv("MODELO_CLAUDE", "claude-haiku-5-5")
DATA_MINIMA = date(2026, 1, 1)          # escopo do painel
FOLGA_DIAS = 3                          # sobreposição entre execuções
MAX_TEXTO = 12000                       # caracteres enviados por notícia
PAUSA_HOST = 0.6                        # segundos entre requisições ao mesmo host
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/124.0 Safari/537.36 coletor-apreensoes/1.0")

PF, PRF, RF = "Polícia Federal", "Polícia Rodoviária Federal", "Receita Federal"
INSTITUICOES = (PF, PRF, RF)
NI = "Não informado"
MODAIS = ("Rodovia", "Aeroporto", "Porto", "Remessa postal", "Fronteira terrestre", "Outro")

# categoria -> (classe, regex de detecção)
SUBSTANCIAS = {
    "Ecstasy/MDMA": ("Droga sintética", r"ecstasy|[êe]xtase|\bmdma\b|\bmda\b"),
    "Metanfetamina": ("Droga sintética", r"metanfetamina|crystal meth"),
    "Anfetamina (rebite)": ("Droga sintética", r"anfetamina|rebite|femproporex|anfepramona"),
    "LSD": ("Droga sintética", r"\blsd\b|[áa]cido lis[ée]rgico|micropontos?"),
    "Selos de composição indefinida": ("Droga sintética", r"\bselos?\b|blotters?"),
    "Cetamina": ("Droga sintética", r"cetamina|ketamina"),
    "GHB": ("Droga sintética", r"\bghb\b|\bgbl\b|gama-?hidroxibut"),
    "Outras sintéticas": ("Droga sintética",
                          r"drogas? sint[ée]ticas?|catinona|canabin[óo]ides? sint|fentanil|nitazeno|2c-?b|\btus[ie]\b|\bk9\b"),
    "Acetato de etila": ("Precursor químico", r"acetato de etila"),
    "Helional": ("Precursor químico", r"helional"),
    "Safrol": ("Precursor químico", r"safrol"),
    "Outro precursor": ("Precursor químico",
                        r"precursor(es)? qu[íi]mico|insumos? qu[íi]micos?|carbonato de c[áa]lcio|permanganato|[ée]ter et[íi]lico"),
}
RE_SUBST = re.compile("|".join(f"(?:{p})" for _, p in SUBSTANCIAS.values()), re.I)
RE_ACAO = re.compile(r"apreen|apreend|ret[ée]m|retid|intercept|flagra|prend|pres[oa]s?\b|localiz|encontr|confisc", re.I)
RE_DESCARTE = re.compile(r"incinera|balan[çc]o anual|palestra|capacita[çc][ãa]o|semin[áa]rio", re.I)
RE_ORGAO = re.compile(r"\bPRF\b|rodovi[áa]ria federal|\bPF\b|pol[íi]cia federal|receita federal|\bFICCO\b|alf[âa]ndega|aduan", re.I)

TERMOS_BUSCA = ["ecstasy", "MDMA", "metanfetamina", "anfetamina", "LSD", "cetamina", "GHB",
                "drogas sintéticas", "acetato de etila", "safrol", "helional", "precursores químicos"]

log = logging.getLogger("coleta")


# ----------------------------------------------------------------------------- utilidades
def sem_acento(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s or "") if unicodedata.category(c) != "Mn").lower().strip()


def para_data(s) -> date | None:
    if not s:
        return None
    if isinstance(s, date):
        return s
    s = str(s).strip()
    for rx, fmt in ((r"\d{4}-\d{2}-\d{2}", "%Y-%m-%d"), (r"\d{2}/\d{2}/\d{4}", "%d/%m/%Y")):
        m = re.search(rx, s)
        if m:
            try:
                return datetime.strptime(m.group(0), fmt).date()
            except ValueError:
                pass
    try:
        return parsedate_to_datetime(s).date()
    except (TypeError, ValueError):
        return None


def data_da_url(url: str) -> date | None:
    m = re.search(r"/(20\d{2})/(\d{2})(?:/(\d{2}))?/", url)
    if not m:
        return None
    return date(int(m.group(1)), int(m.group(2)), int(m.group(3) or 1))


def candidata(texto: str, exige_orgao: bool) -> bool:
    """Filtro barato: substância monitorada + verbo de apreensão (+ órgão federal, para imprensa)."""
    if not texto or RE_DESCARTE.search(texto):
        return False
    if not (RE_SUBST.search(texto) and RE_ACAO.search(texto)):
        return False
    return bool(RE_ORGAO.search(texto)) if exige_orgao else True


class Http:
    """Sessão com retentativas, timeout e cortesia por host."""

    def __init__(self):
        self.s = requests.Session()
        retry = Retry(total=3, backoff_factor=1.5, status_forcelist=(429, 500, 502, 503, 504),
                      allowed_methods=("GET",))
        self.s.mount("https://", HTTPAdapter(max_retries=retry))
        self.s.mount("http://", HTTPAdapter(max_retries=retry))
        self.s.headers.update({"User-Agent": UA, "Accept-Language": "pt-BR,pt;q=0.9"})
        self._ultimo: dict[str, float] = {}

    def get(self, url: str, **kw) -> requests.Response | None:
        host = urlparse(url).netloc
        espera = PAUSA_HOST - (time.monotonic() - self._ultimo.get(host, 0))
        if espera > 0:
            time.sleep(espera)
        try:
            r = self.s.get(url, timeout=kw.pop("timeout", 25), **kw)
            self._ultimo[host] = time.monotonic()
            if r.status_code == 200:
                return r
            log.debug("HTTP %s em %s", r.status_code, url)
        except requests.RequestException as e:
            log.debug("falha em %s: %s", url, e)
        return None


def candidato(url, titulo, resumo, data_pub, fonte, tipo):
    return {"url": url, "titulo": (titulo or "").strip(), "resumo": (resumo or "").strip()[:600],
            "data_pub": data_pub, "fonte": fonte, "tipo": tipo}


# ----------------------------------------------------------------------------- descoberta (fontes)
def _itens_plone(html: str, base: str, padrao_link: str):
    """Extrai (url, título, trecho, data) de listagens/buscas Plone clássicas (PF, Receita)."""
    soup = BeautifulSoup(html, "html.parser")
    vistos = set()
    for a in soup.select("h2 a[href], h3 a[href], a.summary[href], .result-title a[href], .searchResults dt a[href]"):
        url = urljoin(base, a["href"]).split("?")[0]
        if not re.search(padrao_link, url) or url in vistos:
            continue
        vistos.add(url)
        trecho, alvo = [], a.find_parent(["h2", "h3", "dt", "li"]) or a
        for el in alvo.next_elements:
            if getattr(el, "name", None) in ("h2", "h3", "dt") and el is not alvo:
                break
            if isinstance(el, str):
                trecho.append(el.strip())
            if sum(map(len, trecho)) > 700:
                break
        txt = " ".join(t for t in trecho if t)
        m = re.search(r"publicado\s+(\d{2}/\d{2}/\d{4})", txt, re.I)
        yield url, a.get_text(" ", strip=True), txt, (para_data(m.group(1)) if m else data_da_url(url))


def fonte_pf(http: Http, desde: date):
    """Listagem paginada da PF (cobre ~3 meses) + busca interna por termo (alcança meses anteriores)."""
    base, padrao = "https://www.gov.br/pf/pt-br/assuntos/noticias", r"/noticias/20\d{2}/\d{2}/"
    for inicio in range(0, 60 * 40, 60):
        r = http.get(f"{base}?b_start:int={inicio}")
        if not r:
            break
        itens = list(_itens_plone(r.text, base, padrao))
        if not itens:
            break
        for url, tit, txt, dt in itens:
            yield candidato(url, tit, txt, dt, "gov.br/pf", "Oficial")
        datas = [d for *_, d in itens if d]
        if datas and min(datas) < desde:
            break
    yield from _busca_plone(http, "https://www.gov.br/pf", padrao, desde, "gov.br/pf")


def fonte_receita(http: Http, desde: date):
    yield from _busca_plone(http, "https://www.gov.br/receitafederal",
                            r"/noticias/20\d{2}/", desde, "gov.br/receitafederal")


def _busca_plone(http: Http, site: str, padrao: str, desde: date, nome: str):
    for termo in TERMOS_BUSCA:
        for inicio in range(0, 200, 20):
            url = (f"{site}/@@search?SearchableText={quote_plus(termo)}"
                   f"&sort_on=Date&sort_order=reverse&b_start:int={inicio}")
            r = http.get(url)
            if not r:
                break
            itens = list(_itens_plone(r.text, site, padrao))
            if not itens:
                break
            for u, tit, txt, dt in itens:
                if dt is None or dt >= desde - timedelta(days=31):
                    yield candidato(u, tit, txt, dt, nome, "Oficial")
            datas = [d for *_, d in itens if d]
            if datas and max(datas) < desde:
                break


def fonte_prf(http: Http, desde: date):
    """PRF (Plone 6/Volto): a página monta a lista por JavaScript; usamos a API REST que a alimenta."""
    api = "https://www.gov.br/prf/++api++/pt-br/noticias/@search"
    obteve = False
    for termo in TERMOS_BUSCA:
        url = (f"{api}?SearchableText={quote_plus(termo)}&sort_on=effective&sort_order=reverse"
               f"&b_size=50&metadata_fields=effective&metadata_fields=description")
        for _ in range(6):
            r = http.get(url, headers={"Accept": "application/json"})
            if not r:
                break
            try:
                js = r.json()
            except ValueError:
                break
            obteve = True
            itens = js.get("items", [])
            for it in itens:
                u = it.get("@id", "").replace("/++api++", "")
                dt = para_data(it.get("effective"))
                if dt and dt >= desde:
                    yield candidato(u, it.get("title"), it.get("description"), dt, "gov.br/prf", "Oficial")
            datas = [para_data(i.get("effective")) for i in itens]
            datas = [d for d in datas if d]
            nxt = (js.get("batching") or {}).get("next")
            if not nxt or not datas or min(datas) < desde:
                break
            url = nxt
    if not obteve:
        log.warning("PRF: API REST indisponível; tentando navegador (Playwright).")
        yield from _prf_playwright(desde)


def _prf_playwright(desde: date):
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.warning("PRF: instale 'playwright' (pip install playwright && playwright install chromium) "
                    "para usar o plano B. A PRF seguirá coberta pela imprensa.")
        return
    with sync_playwright() as p:
        nav = p.chromium.launch()
        pg = nav.new_page(user_agent=UA)
        pg.goto("https://www.gov.br/prf/pt-br/noticias", wait_until="networkidle", timeout=60000)
        html = pg.content()
        nav.close()
    for url, tit, txt, dt in _itens_plone(html, "https://www.gov.br/prf/pt-br/", r"/noticias/"):
        if not dt or dt >= desde:
            yield candidato(url, tit, txt, dt, "gov.br/prf", "Oficial")


def fonte_sindireceita(http: Http, desde: date):
    url = "https://sindireceita.org.br/noticias/categorias/aduana"
    for _ in range(40):
        r = http.get(url)
        if not r:
            break
        soup = BeautifulSoup(r.text, "html.parser")
        datas = []
        for a in soup.find_all("a", href=re.compile(r"/noticias/20\d{2}/\d{2}/\d{2}/")):
            u = urljoin(url, a["href"])
            dt = data_da_url(u)
            datas.append(dt)
            tit = a.get_text(" ", strip=True)
            if tit and dt and dt >= desde:
                yield candidato(u, tit, "", dt, "sindireceita.org.br", "Imprensa")
        prox = soup.find("a", rel="next") or soup.find("a", string=re.compile(r"Pr[óo]xim|›|»"))
        if not prox or not datas or min(d for d in datas if d) < desde:
            break
        url = urljoin(url, prox["href"])


def fonte_imprensa(http: Http, desde: date):
    """Google Notícias (RSS): cobre portais nacionais e locais com uma consulta por janela de tempo."""
    orgaos = '(PRF OR "Polícia Federal" OR "Receita Federal" OR FICCO)'
    hoje = date.today()
    janelas, ini = [], desde
    while ini <= hoje:                       # janelas mensais: o RSS devolve no máx. ~100 itens
        fim = min(date(ini.year + (ini.month == 12), ini.month % 12 + 1, 1), hoje + timedelta(days=1))
        janelas.append((ini, fim))
        ini = fim
    for termo in TERMOS_BUSCA:
        for a, b in janelas:
            q = f'"{termo}" {orgaos} after:{a.isoformat()} before:{b.isoformat()}'
            r = http.get(f"https://news.google.com/rss/search?q={quote_plus(q)}&hl=pt-BR&gl=BR&ceid=BR:pt-419")
            if not r:
                continue
            try:
                raiz = ET.fromstring(r.content)
            except ET.ParseError:
                continue
            for it in raiz.iter("item"):
                tit = it.findtext("title") or ""
                src = it.find("source")
                yield candidato(it.findtext("link"), re.sub(r"\s+-\s+[^-]+$", "", tit),
                                BeautifulSoup(it.findtext("description") or "", "html.parser").get_text(" "),
                                para_data(it.findtext("pubDate")),
                                (src.get("url") if src is not None else "") or "news.google.com", "Imprensa")


FONTES = {"pf": fonte_pf, "prf": fonte_prf, "receita": fonte_receita,
          "sindireceita": fonte_sindireceita, "imprensa": fonte_imprensa}


# ----------------------------------------------------------------------------- leitura da notícia
def ler_noticia(http: Http, c: dict) -> tuple[str, str]:
    """Devolve (url_final, texto). Para links do Google Notícias tenta resolver o endereço real."""
    url = _resolver_gnews(c["url"])
    r = http.get(url)
    if not r or "news.google.com" in r.url:
        return url, f"{c['titulo']}. {c['resumo']}"          # sem texto completo: usa título+resumo
    html = r.text
    texto = trafilatura.extract(html, include_comments=False) if trafilatura else None
    if not texto:
        soup = BeautifulSoup(html, "html.parser")
        corpo = soup.find("article") or soup.find(id="content") or soup.body or soup
        texto = "\n".join(p.get_text(" ", strip=True) for p in corpo.find_all("p"))
    if not c.get("data_pub"):
        m = re.search(r'article:published_time"\s+content="([^"]+)"', html) or \
            re.search(r"Publicado em\s+(\d{2}/\d{2}/\d{4})", html)
        c["data_pub"] = para_data(m.group(1)) if m else data_da_url(r.url)
    return r.url, f"{c['titulo']}\n\n{texto}"[:MAX_TEXTO]


# ----------------------------------------------------------------------------- extração
PROMPT = f"""Você extrai dados de notícias brasileiras sobre apreensões. Responda SOMENTE com JSON válido.

Escopo: apreensões com participação de Polícia Federal (inclui FICCO), Polícia Rodoviária Federal ou Receita Federal,
envolvendo: {", ".join(SUBSTANCIAS)}. Ignore outras drogas (maconha, cocaína etc.), mas cite-as em "observacoes".
Não são apreensões: incinerações, balanços/estatísticas, operações sem material apreendido descrito → relevante=false.
Uma notícia pode ter várias ocorrências distintas (locais/datas diferentes): uma entrada em "eventos" para cada.

Formato:
{{"relevante": bool, "eventos": [{{
  "data": "AAAA-MM-DD" (data da apreensão; se ausente, a da publicação), "data_aproximada": bool,
  "uf": "SP", "municipio": "Guarulhos",
  "modal": um de {list(MODAIS)},
  "instituicao": a que detectou ou liderou, um de {list(INSTITUICOES)},
  "orgaos_envolvidos": "ex.: Receita Federal + PF",
  "presos": "Sim"|"Não"|"{NI}", "n_presos": int|null, "presos_obs": str|null,
  "arma": "Sim"|"Não"|"{NI}" (munição sem arma = "Não" e descreva em arma_detalhe), "arma_detalhe": str|null,
  "dinheiro": "Sim"|"Não"|"{NI}", "dinheiro_valor": número em reais|null,
  "itens": [{{"substancia": uma das categorias acima, "descricao": str,
             "qtd_kg": número|null (converta g e t para kg), "qtd_unidades": número|null,
             "unidade": "comprimidos"|"frascos"|"ampolas"|"micropontos"|"selos"|"litros"|...|null}}],
  "observacoes": str (curta: contexto, valores aproximados, dúvidas)
}}]}}
Regras: não invente números; "cerca de/quase/mais de" vira o número citado e "aproximado" em observacoes.
"Encaminhado à delegacia" sem menção a prisão → presos="{NI}". Use null quando a notícia não informar."""


class Extrator:
    def __init__(self, usar_llm: bool):
        self.cli = None
        if usar_llm and anthropic and os.getenv("ANTHROPIC_API_KEY"):
            self.cli = anthropic.Anthropic()
        elif usar_llm:
            log.warning("Sem ANTHROPIC_API_KEY ou pacote 'anthropic': usando extração heurística (revisar).")
        self.chamadas = 0

    def __call__(self, texto: str, c: dict) -> dict:
        return self._llm(texto, c) if self.cli else self._heuristica(texto, c)

    def _llm(self, texto, c):
        msg = f"Fonte: {c['fonte']} | publicada em: {c.get('data_pub')}\n\n{texto}"
        for tentativa in range(2):
            try:
                r = self.cli.messages.create(model=MODELO, max_tokens=1800, system=PROMPT,
                                             messages=[{"role": "user", "content": msg}])
                self.chamadas += 1
                bruto = "".join(b.text for b in r.content if getattr(b, "type", "") == "text")
                bruto = re.sub(r"^```(?:json)?|```$", "", bruto.strip(), flags=re.M).strip()
                return json.loads(bruto)
            except json.JSONDecodeError:
                msg += "\n\nATENÇÃO: responda apenas com o JSON, sem texto extra."
            except Exception as e:  # erro de API: não derruba a execução
                log.warning("API falhou (%s): %s", c["url"], e)
                time.sleep(3 * (tentativa + 1))
        return {"relevante": False, "erro": True}

    @staticmethod
    def _heuristica(texto, c):
        """Plano B sem API: captura o essencial e marca para revisão."""
        itens = []
        for cat, (_, rx) in SUBSTANCIAS.items():
            if re.search(rx, texto, re.I):
                kg = un = unidade = None
                m = re.search(r"(\d[\d.]*(?:,\d+)?)\s*(toneladas?|kg|quilos?|gramas?)\b[^.]{0,60}?(?:" + rx + ")",
                              texto, re.I)
                if m:
                    v = float(m.group(1).replace(".", "").replace(",", "."))
                    u = m.group(2).lower()
                    kg = v * 1000 if u.startswith("tonelada") else v / 1000 if u.startswith("grama") else v
                m2 = re.search(r"(\d[\d.]*)\s*(comprimidos|frascos|ampolas|micropontos|selos|cartelas)[^.]{0,40}?(?:"
                               + rx + ")", texto, re.I)
                if m2:
                    un, unidade = float(m2.group(1).replace(".", "")), m2.group(2).lower()
                itens.append({"substancia": cat, "descricao": "extração automática (revisar)",
                              "qtd_kg": kg, "qtd_unidades": un, "unidade": unidade})
        inst = PRF if re.search(r"\bPRF\b|rodovi[áa]ria federal", texto, re.I) else \
            RF if re.search(r"receita federal", texto, re.I) else PF
        loc = re.search(r"\bem ([A-ZÁÉÍÓÚÂÊÔÃÕÇ][\wÀ-ú' ]{2,40})\s*[/(-]\s*([A-Z]{2})\b", texto)
        return {"relevante": bool(itens), "revisar": True, "eventos": [{
            "data": str(c.get("data_pub") or ""), "data_aproximada": True,
            "uf": loc.group(2) if loc else "", "municipio": loc.group(1).strip() if loc else "",
            "modal": "Outro", "instituicao": inst, "orgaos_envolvidos": inst,
            "presos": "Sim" if re.search(r"\bpres[oa]s?\b|pris[ãa]o em flagrante", texto, re.I) else NI,
            "n_presos": None, "arma": NI, "dinheiro": NI, "itens": itens,
            "observacoes": "Extraído sem IA: conferir campos."}]} if itens else {"relevante": False}


# ----------------------------------------------------------------------------- normalização (formato do painel)
def _num(v):
    try:
        return None if v in (None, "") else float(v)
    except (TypeError, ValueError):
        return None


def fmt_num(v: float, casas=3) -> str:
    s = f"{v:,.{casas}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return s.rstrip("0").rstrip(",") if "," in s else s


def linha_painel(ev: dict, item: dict, evento_id: int, seq: int, c: dict, url: str) -> dict | None:
    sub = item.get("substancia")
    if sub not in SUBSTANCIAS or ev.get("instituicao") not in INSTITUICOES:
        return None
    dt = para_data(ev.get("data")) or c.get("data_pub")
    if not dt or dt < DATA_MINIMA:
        return None
    classe = SUBSTANCIAS[sub][0]
    kg, un = _num(item.get("qtd_kg")), _num(item.get("qtd_unidades"))
    unidade = (item.get("unidade") or "unidades") if un is not None else None
    if classe == "Precursor químico" and kg and kg >= 1000:
        qtd = f"≈ {fmt_num(kg / 1000, 1)} t"
    elif kg is not None and un is not None:
        qtd = f"{fmt_num(kg)} kg ({fmt_num(un, 0)} {unidade})"
    elif kg is not None:
        qtd = f"{fmt_num(kg)} kg"
    elif un is not None:
        qtd = f"{fmt_num(un, 0)} {unidade}"
    else:
        qtd = "Não informada"
    presos, n = ev.get("presos") or NI, ev.get("n_presos")
    presos_qtd = ev.get("presos_obs") or (str(n) if n is not None else "0" if presos == "Não"
                                           else "Sim (nº não informado)" if presos == "Sim" else NI)
    arma, adet = ev.get("arma") or NI, ev.get("arma_detalhe")
    din, dval = ev.get("dinheiro") or NI, _num(ev.get("dinheiro_valor"))
    mun, uf = (ev.get("municipio") or "").strip(), (ev.get("uf") or "").strip().upper()
    obs = (ev.get("observacoes") or "").strip()
    if ev.get("data_aproximada"):
        obs = ("Data aproximada. " + obs).strip()
    eid = f"BR{dt:%y}-{evento_id:03d}"
    return {
        "id": f"{eid}-{seq}", "evento_id": eid, "data": dt.isoformat(), "data_br": f"{dt:%d/%m/%Y}",
        "ano": dt.year, "mes": dt.month, "instituicao": ev["instituicao"],
        "orgaos_envolvidos": ev.get("orgaos_envolvidos") or ev["instituicao"],
        "uf": uf, "municipio": mun, "local": f"{mun}/{uf}" if mun else uf,
        "modal": ev.get("modal") if ev.get("modal") in MODAIS else "Outro",
        "substancia": sub, "classe": classe, "sintetica": "Sim", "descricao": item.get("descricao") or "",
        "qtd_kg": kg, "qtd_unidades": un, "unidade": unidade, "quantidade": qtd,
        "presos": presos, "n_presos": n, "presos_qtd": presos_qtd,
        "arma": arma, "arma_detalhe": adet,
        "armas": f"Sim — {adet}" if arma == "Sim" and adet else f"Não ({adet})" if arma == "Não" and adet else arma,
        "dinheiro": din, "dinheiro_valor": dval,
        "dinheiro_txt": f"R$ {fmt_num(dval, 0)}" if dval else "Sim (valor não informado)" if din == "Sim" else din,
        "tipo_fonte": c["tipo"], "fonte_nome": c["fonte"] if "gov.br" in url else (urlparse(url).netloc.replace("www.", "") or c["fonte"]),
        "fonte_url": url, "fontes_secundarias": [], "observacoes": obs[:400],
        "revisar": bool(c.get("revisar")), "data_coleta": date.today().isoformat(),
    }


# ----------------------------------------------------------------------------- deduplicação
def _qtd_compativel(a: dict, b: dict) -> bool:
    for k in ("qtd_kg", "qtd_unidades"):
        x, y = a.get(k), b.get(k)
        if x is not None and y is not None:
            return abs(x - y) <= 0.2 * max(x, y)
    return True  # uma das versões sem quantidade: decide pelo resto


def mesmo_evento(n: dict, e: dict) -> bool:
    if n["fonte_url"] == e["fonte_url"] and n["substancia"] == e["substancia"]:
        return True
    if n["substancia"] != e["substancia"] or n["uf"] != e["uf"]:
        return False
    if abs((para_data(n["data"]) - para_data(e["data"])).days) > 2:
        return False
    sim = difflib.SequenceMatcher(None, sem_acento(n["municipio"]), sem_acento(e["municipio"])).ratio()
    return sim >= 0.85 and _qtd_compativel(n, e)


def fundir(e: dict, n: dict) -> bool:
    """Funde a versão nova na existente. Oficial prevalece; campos vazios são completados."""
    mudou = False
    if n["tipo_fonte"] == "Oficial" and e["tipo_fonte"] != "Oficial":
        e.setdefault("fontes_secundarias", []).append(e["fonte_url"])
        for k in ("fonte_url", "fonte_nome", "tipo_fonte"):
            e[k] = n[k]
        mudou = True
    elif n["fonte_url"] not in [e["fonte_url"], *e.get("fontes_secundarias", [])]:
        e.setdefault("fontes_secundarias", []).append(n["fonte_url"])
        mudou = True
    for k, ks in (("presos", ("n_presos", "presos_qtd")), ("arma", ("arma_detalhe", "armas")),
                  ("dinheiro", ("dinheiro_valor", "dinheiro_txt"))):
        if e.get(k) in (None, NI) and n.get(k) not in (None, NI):
            e[k] = n[k]
            for kk in ks:
                e[kk] = n[kk]
            mudou = True
    for k in ("qtd_kg", "qtd_unidades"):
        if e.get(k) is None and n.get(k) is not None:
            e[k], e["quantidade"], e["unidade"] = n[k], n["quantidade"], n.get("unidade") or e.get("unidade")
            mudou = True
    return mudou


# ----------------------------------------------------------------------------- persistência
def carregar():
    base = json.loads(ARQ_BASE.read_text("utf-8")) if ARQ_BASE.exists() else []
    if ARQ_ESTADO.exists():
        est = json.loads(ARQ_ESTADO.read_text("utf-8"))
    else:  # primeira execução: estado derivado da base
        est = {"ultima_execucao": None, "urls_vistas": sorted({r["fonte_url"] for r in base})}
    nums = [int(m.group(1)) for r in base if (m := re.search(r"-(\d+)$", r["evento_id"]))]
    est["ultimo_evento"] = max([est.get("ultimo_evento", 0), *nums, 0])
    return base, est


def gravar(base: list, est: dict, relatorio: str):
    base.sort(key=lambda r: (r["data"], r["id"]))
    ARQ_BASE.parent.mkdir(parents=True, exist_ok=True)
    ARQ_BASE.write_text(json.dumps(base, ensure_ascii=False, indent=1), "utf-8")
    campos = ["id", "data_br", "instituicao", "municipio", "uf", "modal", "classe", "substancia", "quantidade",
              "presos_qtd", "armas", "dinheiro_txt", "tipo_fonte", "fonte_url", "observacoes", "revisar"]
    with ARQ_CSV.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=campos, extrasaction="ignore", delimiter=";")
        w.writeheader()
        w.writerows(base)
    est["urls_vistas"] = est["urls_vistas"][-30000:]
    ARQ_ESTADO.write_text(json.dumps(est, ensure_ascii=False, indent=1), "utf-8")
    DIR_REL.mkdir(exist_ok=True)
    (DIR_REL / f"coleta_{datetime.now():%Y-%m-%d_%H%M}.md").write_text(relatorio, "utf-8")


# ----------------------------------------------------------------------------- execução
def _periodo(args, est) -> date:
    if args.desde:
        desde = para_data(args.desde)
    elif est.get("ultima_execucao"):
        desde = para_data(est["ultima_execucao"]) - timedelta(days=FOLGA_DIAS)
    else:
        desde = DATA_MINIMA
    return max(desde, DATA_MINIMA)


def descobrir(http: Http, fontes: str, desde: date, vistas: set, cont: dict) -> list:
    """Roda as fontes e devolve só as candidatas inéditas (oficiais primeiro)."""
    fila, na_fila = [], set()
    for nome in fontes.split(","):
        fn = FONTES.get(nome.strip())
        if not fn:
            continue
        n0 = len(fila)
        try:
            for c in fn(http, desde):
                cont["descobertas"] += 1
                if not c["url"] or c["url"] in vistas or c["url"] in na_fila:
                    continue
                if c.get("data_pub") and c["data_pub"] < desde - timedelta(days=31):
                    continue
                if candidata(f"{c['titulo']} {c['resumo']}", exige_orgao=c["tipo"] == "Imprensa"):
                    na_fila.add(c["url"])
                    fila.append(c)
        except Exception as e:  # uma fonte com problema não derruba as demais
            log.warning("%s: interrompida (%s)", nome, e)
        log.info("%-12s %d candidatas", nome, len(fila) - n0)
    cont["candidatas"] = len(fila)
    fila.sort(key=lambda c: c["tipo"] != "Oficial")
    return fila


def executar(args):
    base, est = carregar()
    vistas = set(est["urls_vistas"])
    desde = _periodo(args, est)
    log.info("Coletando desde %s", desde)
    http = Http()
    cont = {"descobertas": 0, "candidatas": 0, "lidas": 0, "novas": 0, "fundidas": 0, "irrelevantes": 0}
    fila = descobrir(http, args.fontes, desde, vistas, cont)
    if args.so_links:
        return modo_links(http, fila, est, desde, cont, args.seco)

    extrair = Extrator(not args.sem_llm)
    novas, fundidas, revisar = [], [], []
    for c in fila[: args.max]:
        url, texto = ler_noticia(http, c)
        cont["lidas"] += 1
        repetida = url != c["url"] and url in vistas      # mesmo artigo achado por outro caminho
        vistas.update({c["url"], url})
        if repetida:
            continue
        res = extrair(texto, c)
        c["revisar"] = res.get("revisar", False)
        if not res.get("relevante"):
            cont["irrelevantes"] += 1
            continue
        for ev in res.get("eventos", []):
            est["ultimo_evento"] += 1
            linhas = [l for i, it in enumerate(ev.get("itens", []), 1)
                      if (l := linha_painel(ev, it, est["ultimo_evento"], i, c, url))]
            alvo_novo = False
            for l in linhas:
                existente = next((e for e in base if mesmo_evento(l, e)), None)
                if existente:
                    if fundir(existente, l):
                        fundidas.append(existente)
                        cont["fundidas"] += 1
                else:
                    base.append(l)
                    novas.append(l)
                    alvo_novo = True
                    if l["revisar"]:
                        revisar.append(l)
            if not alvo_novo:
                est["ultimo_evento"] -= 1   # evento já existia (ou vazio): não consome número
    cont["novas"] = len(novas)

    est["urls_vistas"] = sorted(vistas)
    est["ultima_execucao"] = date.today().isoformat()
    rel = relatorio(desde, cont, novas, fundidas, revisar, extrair.chamadas, len(fila) > args.max)
    if args.seco:
        print(rel)
    else:
        gravar(base, est, rel)
        log.info("Base: %d linhas (%d novas, %d fundidas). Arquivo: %s", len(base), len(novas), len(fundidas), ARQ_BASE)


# ----------------------------------------------------------------------------- modo sem custo
def _resolver_gnews(url: str) -> str:
    if "news.google.com" not in url:
        return url
    try:
        from googlenewsdecoder import gnewsdecoder  # opcional
        d = gnewsdecoder(url)
        return d["decoded_url"] if d.get("status") else url
    except Exception:
        return url


def modo_links(http, fila, est, desde, cont, seco):
    """Sem API: acumula as candidatas em links/pendentes.(json|md) para extração no chat do Claude."""
    pend = json.loads(PEND_JSON.read_text("utf-8")) if PEND_JSON.exists() else []
    ja = {p["url"] for p in pend} | {p.get("url_original") for p in pend}
    novos = []
    for c in fila:
        url = _resolver_gnews(c["url"])
        if url in ja or c["url"] in ja or url in est["urls_vistas"]:
            continue
        ja.add(url)
        texto = f"{c['titulo']} {c['resumo']}"
        novos.append({"url": url, "url_original": c["url"], "titulo": c["titulo"],
                      "data_pub": c["data_pub"].isoformat() if c.get("data_pub") else "",
                      "fonte": c["fonte"], "tipo": c["tipo"],
                      "substancias": [k for k, (_, rx) in SUBSTANCIAS.items() if re.search(rx, texto, re.I)],
                      "descoberto_em": date.today().isoformat()})
    pend += novos
    est["urls_vistas"] = sorted(set(est["urls_vistas"]) | {n["url_original"] for n in novos} | {n["url"] for n in novos})
    est["ultima_execucao"] = date.today().isoformat()
    md = _pendentes_md(pend)
    log.info("Links: %d novos nesta execução; %d pendentes no total (desde %s).", len(novos), len(pend), desde)
    if seco:
        print(md)
        return
    DIR_LINKS.mkdir(exist_ok=True)
    PEND_JSON.write_text(json.dumps(pend, ensure_ascii=False, indent=1), "utf-8")
    PEND_MD.write_text(md, "utf-8")
    est["urls_vistas"] = est["urls_vistas"][-30000:]
    ARQ_ESTADO.parent.mkdir(parents=True, exist_ok=True)
    ARQ_ESTADO.write_text(json.dumps(est, ensure_ascii=False, indent=1), "utf-8")


def _pendentes_md(pend: list) -> str:
    ordem = sorted(pend, key=lambda p: (p["tipo"] != "Oficial", p["data_pub"] or ""), reverse=False)
    L = [f"# Notícias candidatas — {len(pend)} pendentes (atualizado em {datetime.now():%d/%m/%Y %H:%M})", "",
         "Cole este conteúdo no chat do Claude e peça: “extraia as apreensões destes links e atualize o painel”.", ""]
    for tipo in ("Oficial", "Imprensa"):
        grupo = [p for p in ordem if p["tipo"] == tipo]
        if not grupo:
            continue
        L += [f"## {'Fontes oficiais' if tipo == 'Oficial' else 'Imprensa e sindicatos'} ({len(grupo)})", ""]
        for p in sorted(grupo, key=lambda p: p["data_pub"] or "", reverse=True):
            dt = para_data(p["data_pub"])
            L.append(f"- {dt:%d/%m/%Y} · {p['fonte']} · {p['titulo']} · {', '.join(p['substancias']) or '—'}  \n  {p['url']}"
                     if dt else f"- s/d · {p['fonte']} · {p['titulo']}  \n  {p['url']}")
        L.append("")
    return "\n".join(L)


def limpar_pendentes():
    """Arquiva a lista entregue e começa uma nova (as URLs continuam em estado.json e não voltam)."""
    if not PEND_JSON.exists():
        log.info("Nada a limpar.")
        return
    arq = DIR_LINKS / "entregues"
    arq.mkdir(parents=True, exist_ok=True)
    carimbo = f"{datetime.now():%Y-%m-%d_%H%M}"
    PEND_JSON.rename(arq / f"pendentes_{carimbo}.json")
    if PEND_MD.exists():
        PEND_MD.rename(arq / f"pendentes_{carimbo}.md")
    log.info("Pendentes arquivados em %s", arq)


def relatorio(desde, cont, novas, fundidas, revisar, chamadas, truncado):
    L = [f"# Coleta de {datetime.now():%d/%m/%Y %H:%M}", "",
         f"Período: desde {desde:%d/%m/%Y}. Descobertas: {cont['descobertas']}; candidatas: {cont['candidatas']}; "
         f"lidas: {cont['lidas']}; irrelevantes: {cont['irrelevantes']}; chamadas à API: {chamadas}.", ""]
    if truncado:
        L += ["**Atenção:** limite de leitura atingido (--max); rode de novo para continuar.", ""]
    L += [f"## Novas ({len(novas)})"] + [
        f"- {n['data_br']} · {n['instituicao']} · {n['local']} · {n['substancia']} · {n['quantidade']} · "
        f"presos: {n['presos_qtd']} · [{n['fonte_nome']}]({n['fonte_url']})" for n in novas] + [""]
    L += [f"## Atualizadas por outra fonte ({len(fundidas)})"] + [
        f"- {f['id']} · {f['local']} · {f['substancia']} (+ fonte)" for f in fundidas] + [""]
    if revisar:
        L += [f"## Para revisar ({len(revisar)})"] + [f"- {r['id']} · {r['fonte_url']}" for r in revisar]
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--desde", help="data inicial AAAA-MM-DD (padrão: última execução − 3 dias)")
    ap.add_argument("--fontes", default=",".join(FONTES), help=f"lista separada por vírgulas: {','.join(FONTES)}")
    ap.add_argument("--max", type=int, default=400, help="máximo de notícias lidas por execução")
    ap.add_argument("--sem-llm", action="store_true", help="não usar a API (extração heurística)")
    ap.add_argument("--seco", action="store_true", help="não gravar; só mostrar o relatório")
    ap.add_argument("--so-links", action="store_true",
                    help="sem custo: só descobre e filtra; acumula as candidatas em links/pendentes.md")
    ap.add_argument("--limpar-pendentes", action="store_true", help="arquiva links/pendentes.* e encerra")
    ap.add_argument("-v", "--verboso", action="store_true")
    args = ap.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verboso else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    if args.limpar_pendentes:
        limpar_pendentes()
    else:
        executar(args)


if __name__ == "__main__":
    main()
