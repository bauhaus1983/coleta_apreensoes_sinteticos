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
EXTRA_JSON = DIR_LINKS / "fontes_extras.json"   # {evento_id: [{url, titulo, fonte}]} para casos já no painel
MAX_COMPLEMENTAR = 25                           # buscas complementares por execução

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
    url = _resolver_gnews(c["url"], http)
    r = http.get(url)
    if not r or "news.google.com" in r.url:
        return url, f"{c['titulo']}. {c['resumo']}"          # sem texto completo: usa título+resumo
    html = r.text
    texto = extrair_texto(html)
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
    elif kg is not None and un is not None and sub == "Ecstasy/MDMA" and str(unidade).startswith("comprimido"):
        qtd = f"{fmt_num(un, 0)} {unidade} ({fmt_num(kg)} kg)"   # regra do painel: ecstasy conta pelos comprimidos
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
        "fonte_url": url, "fontes_secundarias": [], "fontes_extra": [], "observacoes": obs[:400],
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
    else:  # primeira execução
        est = {"ultima_execucao": None, "urls_vistas": []}
    urls_base = {r["fonte_url"] for r in base}
    for r in base:  # URLs citadas como fontes secundárias também contam como vistas
        urls_base.update(u for u in r.get("fontes_secundarias", []) if u)
    est["urls_vistas"] = sorted(set(est.get("urls_vistas", [])) | urls_base)
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
        return modo_links(http, fila, est, desde, cont, args.seco, base)

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
GN_STATUS = {"ok": 0, "falha": 0}
GN_SOCS = "CAESEwgDEgk0ODE3Nzk3MjQaAmVuIAEaBgiA_LyaBg"   # registro de consentimento: evita a tela de cookies do Google


def _gn_id(url: str):
    m = re.search(r"news\.google\.com/(?:rss/)?(?:articles|read)/([^?/#]+)", url or "")
    return m.group(1) if m else None


def _resolver_gnews(url: str, http: "Http") -> str:
    """Converte link do Google Notícias no endereço real da matéria, sem pacotes externos.
    1) abre a página do artigo e lê a assinatura (data-n-a-sg) e o carimbo (data-n-a-ts);
    2) consulta o serviço batchexecute do Google, que devolve a URL do veículo."""
    gid = _gn_id(url)
    if not gid:
        return url
    try:
        ts = sig = None
        for base in ("https://news.google.com/articles/", "https://news.google.com/rss/articles/"):
            r = http.get(base + gid, cookies={"SOCS": GN_SOCS})
            if not r:
                continue
            el = BeautifulSoup(r.text, "html.parser").find(attrs={"data-n-a-sg": True})
            if el:
                ts, sig = el.get("data-n-a-ts"), el.get("data-n-a-sg")
                break
        if ts and sig:
            req = ["Fbv4je", '["garturlreq",[["X","X",["X","X"],null,null,1,1,"US:en",null,1,null,null,null,null,null,0,1],'
                             f'"X","X",1,[1,1,1],1,1,null,0,0,null,0],"{gid}",{ts},"{sig}"]']
            resp = http.s.post("https://news.google.com/_/DotsSplashUi/data/batchexecute",
                               data="f.req=" + quote_plus(json.dumps([[req]])),
                               headers={"Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"},
                               cookies={"SOCS": GN_SOCS}, timeout=25)
            dados = json.loads(resp.text.split("\n\n")[1])[:-2]
            real = json.loads(dados[0][2])[1]
            if real and real.startswith("http"):
                GN_STATUS["ok"] += 1
                return real
    except Exception as e:
        log.debug("gnews %s: %s", gid, e)
    GN_STATUS["falha"] += 1
    return url


def testar_gnews():
    """Autoteste: pega 3 links reais do Google Notícias e tenta convertê-los."""
    http = Http()
    r = http.get("https://news.google.com/rss/search?q=" + quote_plus("PRF apreende drogas") + "&hl=pt-BR&gl=BR&ceid=BR:pt-419")
    if not r:
        print("AUTOTESTE: não foi possível ler o RSS do Google Notícias.")
        return
    links = [it.findtext("link") for it in ET.fromstring(r.content).iter("item")][:3]
    for u in links:
        real = _resolver_gnews(u, http)
        print(("OK    " if "news.google.com" not in real else "FALHA ") + real[:120])
    print(f"AUTOTESTE Google Notícias: {GN_STATUS['ok']} de {len(links)} convertidos.")


def extrair_texto(html: str) -> str:
    texto = trafilatura.extract(html, include_comments=False) if trafilatura else None
    if not texto:
        soup = BeautifulSoup(html, "html.parser")
        corpo = soup.find("article") or soup.find(id="content") or soup.body or soup
        texto = "\n".join(t for p in corpo.find_all("p") if len(t := p.get_text(" ", strip=True)) > 40)
    return re.sub(r"\s+", " ", texto or "").strip()


def _ja_na_base(texto: str, data_pub, substancias: list, base: list):
    """Devolve o evento da base que a notícia provavelmente repete (mesma substância, cidade citada, data próxima)."""
    if not data_pub:
        return None
    t = sem_acento(texto)
    for r in base:
        if r["substancia"] not in substancias:
            continue
        mun = sem_acento(r.get("municipio", ""))
        if len(mun) < 4 or mun.startswith("divers") or mun not in t:
            continue
        dr = para_data(r["data"])
        if dr and abs((data_pub - dr).days) <= 5:
            return r["evento_id"]
    return None


def modo_links(http, fila, est, desde, cont, seco, base):
    """Sem API: descobre, descarta o que já está na base, lê um trecho e acumula em links/pendentes.(json|md)."""
    pend = json.loads(PEND_JSON.read_text("utf-8")) if PEND_JSON.exists() else []
    ja = {p["url"] for p in pend} | {p.get("url_original") for p in pend}
    novos, repetidos = [], []
    for c in fila:
        url = _resolver_gnews(c["url"], http)
        if url in ja or c["url"] in ja or url in est["urls_vistas"]:
            continue
        ja.add(url)
        dp = c.get("data_pub")
        base_txt = f"{c['titulo']} {c['resumo']}"
        subs = [k for k, (_, rx) in SUBSTANCIAS.items() if re.search(rx, base_txt, re.I)]
        dup = _ja_na_base(base_txt, dp, subs, base)
        trecho = ""
        if not dup and "news.google.com" not in url:
            r = http.get(url)
            if r:
                txt = extrair_texto(r.text)
                trecho = txt[:600]
                subs = subs or [k for k, (_, rx) in SUBSTANCIAS.items() if re.search(rx, txt[:3000], re.I)]
                dup = _ja_na_base(f"{base_txt} {txt[:3000]}", dp, subs, base)
        item = {"url": url, "url_original": c["url"], "titulo": c["titulo"],
                "data_pub": dp.isoformat() if dp else "", "fonte": c["fonte"], "tipo": c["tipo"],
                "substancias": subs, "trecho": trecho, "descoberto_em": date.today().isoformat()}
        if dup:
            item["evento_base"] = dup
            repetidos.append(item)
        else:
            novos.append(item)
    # casos novos com uma única fonte: procura outras matérias sobre o mesmo caso
    feitos = 0
    for g in agrupar(novos):
        if len(g) > 1 or feitos >= MAX_COMPLEMENTAR:
            continue
        h = g[0]; feitos += 1
        for f in fontes_complementares(http, h["titulo"], para_data(h["data_pub"]), ja | set(est["urls_vistas"])):
            ja.add(f["url"])
            novos.append({**h, "url": f["url"], "url_original": f["url"], "titulo": f["titulo"], "fonte": f["fonte"],
                          "tipo": "Imprensa", "trecho": "", "complementar": True})
    log.info("Busca complementar: %d casos consultados.", feitos)
    pend += novos
    extras = json.loads(EXTRA_JSON.read_text("utf-8")) if EXTRA_JSON.exists() else {}
    for r in repetidos:                      # notícia de caso já registrado = fonte extra desse caso
        lst = extras.setdefault(r["evento_base"], [])
        if r["url"] not in {x["url"] for x in lst}:
            lst.append({"url": r["url"], "titulo": r["titulo"], "fonte": r["fonte"]})
    vistos = {n["url_original"] for n in novos + repetidos} | {n["url"] for n in novos + repetidos}
    est["urls_vistas"] = sorted(set(est["urls_vistas"]) | vistos)
    est["ultima_execucao"] = date.today().isoformat()
    md = _pendentes_md(pend, extras)
    log.info("Links: %d novos; %d descartados por já estarem no painel; %d pendentes no total (desde %s).",
             len(novos), len(repetidos), len(pend), desde)
    log.info("Google Notícias: %d links convertidos, %d falhas.", GN_STATUS["ok"], GN_STATUS["falha"])
    if seco:
        print(md)
        return
    DIR_LINKS.mkdir(exist_ok=True)
    PEND_JSON.write_text(json.dumps(pend, ensure_ascii=False, indent=1), "utf-8")
    PEND_MD.write_text(md, "utf-8")
    EXTRA_JSON.write_text(json.dumps(extras, ensure_ascii=False, indent=1), "utf-8")
    if repetidos:  # registro do que foi descartado, para auditoria
        arq = DIR_LINKS / "descartados_ja_no_painel.json"
        antigos = json.loads(arq.read_text("utf-8")) if arq.exists() else []
        arq.write_text(json.dumps((antigos + repetidos)[-2000:], ensure_ascii=False, indent=1), "utf-8")
    est["urls_vistas"] = est["urls_vistas"][-30000:]
    ARQ_ESTADO.parent.mkdir(parents=True, exist_ok=True)
    ARQ_ESTADO.write_text(json.dumps(est, ensure_ascii=False, indent=1), "utf-8")


_PARADAS = set("de da do das dos em no na nos nas com por para e o a os as um uma apos sobre mais que pela pelo ao".split())


def _palavras(t: str) -> set:
    return {w for w in re.findall(r"[a-z0-9]+", sem_acento(t)) if len(w) > 2 and w not in _PARADAS}


def agrupar(pend: list) -> list:
    """Junta notícias do mesmo caso: substância em comum, datas a até 3 dias e títulos parecidos."""
    grupos = []
    for p in sorted(pend, key=lambda x: (x["tipo"] != "Oficial", x["data_pub"] or "")):
        wp, dp = _palavras(p["titulo"] + " " + p.get("trecho", "")[:200]), para_data(p["data_pub"])
        alvo = None
        for g in grupos:
            h = g[0]
            dh = para_data(h["data_pub"])
            if dp and dh and abs((dp - dh).days) > 3:
                continue
            if set(p["substancias"]) and set(h["substancias"]) and not set(p["substancias"]) & set(h["substancias"]):
                continue
            wh = _palavras(h["titulo"] + " " + h.get("trecho", "")[:200])
            if wp and wh and len(wp & wh) / len(wp | wh) >= 0.3:
                alvo = g
                break
        (alvo.append(p) if alvo else grupos.append([p]))
    return grupos


def _pendentes_md(pend: list, extras: dict | None = None) -> str:
    extras = extras or {}
    grupos = agrupar(pend)
    grupos.sort(key=lambda g: max(x["data_pub"] or "" for x in g), reverse=True)
    L = [f"# Notícias candidatas — {len(grupos)} casos, {len(pend)} links (atualizado em {datetime.now():%d/%m/%Y %H:%M})", "",
         "Cole este conteúdo no chat do Claude e peça: “extraia as apreensões destes casos e atualize o painel”.",
         "Notícias sobre casos que já estão no painel foram descartadas automaticamente.", ""]
    for i, g in enumerate(grupos, 1):
        h = g[0]
        dt = para_data(h["data_pub"])
        L.append(f"## {i}. {dt:%d/%m/%Y} · {h['titulo']}" if dt else f"## {i}. s/d · {h['titulo']}")
        L.append(f"Substâncias citadas: {', '.join(h['substancias']) or '—'} · {len(g)} fonte(s)")
        if h.get("trecho"):
            L.append(f"> {h['trecho']}")
        for p in g:
            L.append(f"- [{p['tipo']}{' · busca complementar' if p.get('complementar') else ''}] {p['fonte']}: {p['url']}")
        L.append("")
    if extras:
        L += ["---", f"# Fontes extras para casos já no painel ({sum(len(v) for v in extras.values())} links)", "",
              "Acrescentar como fontes adicionais (com link) dos casos indicados.", ""]
        for eid in sorted(extras):
            L.append(f"## {eid}")
            L += [f"- {x['fonte']}: {x['url']}  ({x['titulo']})" for x in extras[eid]]
            L.append("")
    return "\n".join(L)


# ----------------------------------------------------------------------------- fontes complementares
PALAVRA_SUB = {"Ecstasy/MDMA": "ecstasy", "Metanfetamina": "metanfetamina", "Anfetamina (rebite)": "rebite",
               "LSD": "LSD", "Selos de composição indefinida": "selos", "Cetamina": "cetamina", "GHB": "GHB",
               "Outras sintéticas": "drogas sintéticas", "Acetato de etila": "acetato de etila", "Helional": "helional",
               "Safrol": "safrol", "Outro precursor": "insumo químico"}
ORG_CURTO = {PF: '"Polícia Federal"', PRF: "PRF", RF: '"Receita Federal"'}


def _gn_busca(http, consulta: str, ini: date, fim: date) -> list:
    q = f"{consulta} after:{ini.isoformat()} before:{fim.isoformat()}"
    r = http.get(f"https://news.google.com/rss/search?q={quote_plus(q)}&hl=pt-BR&gl=BR&ceid=BR:pt-419")
    if not r:
        return []
    try:
        raiz = ET.fromstring(r.content)
    except ET.ParseError:
        return []
    out = []
    for it in raiz.iter("item"):
        src = it.find("source")
        out.append({"link": it.findtext("link"), "titulo": re.sub(r"\s+-\s+[^-]+$", "", it.findtext("title") or ""),
                    "fonte": urlparse(src.get("url")).netloc.replace("www.", "") if src is not None and src.get("url") else ""})
    return out


def fontes_complementares(http, titulo: str, data_pub, excluir: set, max_n: int = 3) -> list:
    """Outras matérias sobre o mesmo caso: busca pelas palavras do título, em ±3 dias, e exige título parecido."""
    if not data_pub:
        return []
    alvo = _palavras(titulo)
    chave = sorted((w for w in alvo if not w.isdigit()), key=len, reverse=True)[:5]
    if len(chave) < 3:
        return []
    out = []
    for it in _gn_busca(http, " ".join(chave), data_pub - timedelta(days=3), data_pub + timedelta(days=4)):
        w = _palavras(it["titulo"])
        if not w or len(w & alvo) / len(w | alvo) < 0.3:
            continue
        url = _resolver_gnews(it["link"], http)
        if "news.google.com" in url or url in excluir or url in {x["url"] for x in out}:
            continue
        out.append({"url": url, "titulo": it["titulo"], "fonte": it["fonte"] or urlparse(url).netloc.replace("www.", "")})
        if len(out) >= max_n:
            break
    return out


def completar_fontes(seco: bool = False, max_casos: int = 80):
    """Procura 2ª (e 3ª) fonte para os casos do painel que têm só um link. Resultado vai para links/pendentes.md."""
    base, est = carregar()
    http = Http()
    extras = json.loads(EXTRA_JSON.read_text("utf-8")) if EXTRA_JSON.exists() else {}
    eventos = {}
    for r in base:
        eventos.setdefault(r["evento_id"], r)
    feitos = achados = 0
    for eid, r in sorted(eventos.items(), key=lambda x: x[1]["data"], reverse=True):
        urls = {r.get("fonte_url")} | {f["url"] if isinstance(f, dict) else f for f in r.get("fontes_extra", []) + r.get("fontes_secundarias", [])}
        urls |= {x["url"] for x in extras.get(eid, [])}
        urls.discard(None)
        mun = r.get("municipio", "")
        if len(urls) >= 2 or len(mun) < 3 or sem_acento(mun).startswith("divers") or feitos >= max_casos:
            continue
        feitos += 1
        d = para_data(r["data"])
        consulta = f'{PALAVRA_SUB.get(r["substancia"], "")} "{mun}" {ORG_CURTO.get(r["instituicao"], "")}'
        rx_sub = SUBSTANCIAS.get(r["substancia"], ("", r"."))[1]
        for it in _gn_busca(http, consulta, d - timedelta(days=3), d + timedelta(days=6)):
            t = it["titulo"]
            if sem_acento(mun) not in sem_acento(t) or not re.search(rx_sub + r"|apreen|droga", t, re.I):
                continue
            url = _resolver_gnews(it["link"], http)
            if "news.google.com" in url or url in urls:
                continue
            urls.add(url); achados += 1
            extras.setdefault(eid, []).append({"url": url, "titulo": t, "fonte": it["fonte"] or urlparse(url).netloc.replace("www.", "")})
            if len(urls) >= 3:
                break
    log.info("Completar fontes: %d casos consultados; %d links encontrados.", feitos, achados)
    pend = json.loads(PEND_JSON.read_text("utf-8")) if PEND_JSON.exists() else []
    md = _pendentes_md(pend, extras)
    if seco:
        print(md); return
    DIR_LINKS.mkdir(exist_ok=True)
    EXTRA_JSON.write_text(json.dumps(extras, ensure_ascii=False, indent=1), "utf-8")
    PEND_JSON.write_text(json.dumps(pend, ensure_ascii=False, indent=1), "utf-8")
    PEND_MD.write_text(md, "utf-8")


def limpar_pendentes():
    """Arquiva a lista entregue e começa uma nova (as URLs continuam em estado.json e não voltam)."""
    if not PEND_JSON.exists() and EXTRA_JSON.exists():
        PEND_JSON.write_text("[]", "utf-8")
    if not PEND_JSON.exists():
        log.info("Nada a limpar.")
        return
    arq = DIR_LINKS / "entregues"
    arq.mkdir(parents=True, exist_ok=True)
    carimbo = f"{datetime.now():%Y-%m-%d_%H%M}"
    PEND_JSON.rename(arq / f"pendentes_{carimbo}.json")
    if EXTRA_JSON.exists():
        EXTRA_JSON.rename(arq / f"fontes_extras_{carimbo}.json")
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
    ap.add_argument("--completar-fontes", action="store_true", help="procura 2ª fonte para casos do painel com um só link")
    ap.add_argument("--testar-gnews", action="store_true", help="autoteste da conversão de links do Google Notícias")
    ap.add_argument("-v", "--verboso", action="store_true")
    args = ap.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verboso else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    if args.testar_gnews:
        testar_gnews()
    elif args.completar_fontes:
        completar_fontes(args.seco)
    elif args.limpar_pendentes:
        limpar_pendentes()
    else:
        executar(args)


if __name__ == "__main__":
    main()
